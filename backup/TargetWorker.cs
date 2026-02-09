using System.Threading.Channels;

namespace backup;

public enum ChangeKind
{
    EnsureDir,
    CopyFile,
    DeleteFile,
    DeleteDir,
    CreateSymlink
}

public sealed record ChangeEvent(
    ChangeKind Kind,
    string RelativePath,
    string? SourceFullPath = null,
    string? LinkTarget = null,
    bool IsDirectoryLink = false
);

public sealed class TargetWorker : IAsyncDisposable
{
    public string SourceRoot { get; }
    public string TargetRoot { get; }

    private readonly Channel<ChangeEvent> Ch;
    private readonly CancellationTokenSource Cts = new();
    private readonly Task Runner;

    private readonly SemaphoreSlim Sem = new(4, 4);

    public TargetWorker(string sourceRoot, string targetRoot, int channelCapacity = 10000)
    {
        SourceRoot = sourceRoot;
        TargetRoot = targetRoot;

        Ch = Channel.CreateBounded<ChangeEvent>(new BoundedChannelOptions(channelCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        Runner = Task.Run(() => RunAsync(Cts.Token));
    }

    public ValueTask PushAsync(ChangeEvent ev, CancellationToken ct = default)
    {
        return Ch.Writer.WriteAsync(ev, ct);
    }

    public void Complete()
    {
        Ch.Writer.TryComplete();
    }

    public async Task StopAsync()
    {
        Ch.Writer.TryComplete();

        try
        {
            await Runner.ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        finally
        {
            Cts.Cancel();
        }
    }

    public async Task RunAsync(CancellationToken ct)
    {
        await foreach (var ev in Ch.Reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            await ApplyAsync(ev, ct).ConfigureAwait(false);
        }
    }

    private async Task ApplyAsync(ChangeEvent ev, CancellationToken ct)
    {
        string destFullPath = Path.Combine(TargetRoot, ev.RelativePath);

        switch (ev.Kind)
        {
            case ChangeKind.EnsureDir:
                if(!Directory.Exists(destFullPath))
                {
                    RemoveAny(destFullPath);
                    Directory.CreateDirectory(destFullPath);
                }
                break;
            case ChangeKind.CopyFile:
                if (ev.SourceFullPath is null)
                {
                    throw new InvalidOperationException("Copyfile requires SourceFullPath");
                }

                string? destDir = Path.GetDirectoryName(destFullPath);
                if (!string.IsNullOrEmpty(destDir))
                {
                    Directory.CreateDirectory(destDir);
                }
                
                RemoveAny(destFullPath);
                await CopyFileAsync(ev.SourceFullPath, destFullPath, ct).ConfigureAwait(false);
                break;
            case ChangeKind.DeleteFile:
                RemoveAny(destFullPath);
                break;
            case ChangeKind.DeleteDir:
                RemoveAny(destFullPath);
                break;
            case ChangeKind.CreateSymlink:
                if (ev.LinkTarget == null)
                {
                    throw new InvalidOperationException("Symlink requires LinkTarget");
                }
                string? parent = Path.GetDirectoryName(destFullPath);
                if (!string.IsNullOrEmpty(parent))
                {
                    Directory.CreateDirectory(parent);
                }
                if (File.Exists(destFullPath))
                {
                    File.Delete(destFullPath);
                }
                else if (Directory.Exists(destFullPath))
                {
                    Directory.Delete(destFullPath, recursive: true);
                }
                RemoveAny(destFullPath);
                if (ev.IsDirectoryLink)
                {
                    Directory.CreateSymbolicLink(destFullPath, ev.LinkTarget);
                }
                else
                {
                    File.CreateSymbolicLink(destFullPath, ev.LinkTarget);
                }

                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(ev.Kind), ev.Kind, "Unknown change kind");
        }
    }

    private async Task CopyFileAsync(string source, string dest, CancellationToken ct)
    {
        await Sem.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 131072, options: FileOptions.Asynchronous | FileOptions.SequentialScan);
            await using var dst = new FileStream(dest, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 131072, options: FileOptions.Asynchronous);
            await src.CopyToAsync(dst, bufferSize: 1024 * 128, cancellationToken: ct).ConfigureAwait(false);
            File.SetLastWriteTimeUtc(dest, File.GetLastWriteTimeUtc(source));
        }
        finally
        {
            Sem.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        Cts.Dispose();
        Sem.Dispose();
    }

    static void RemoveAny(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch { }
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }
        catch { }
    }
}