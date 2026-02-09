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
        Cts.Cancel();

        try
        {
            await Runner.ConfigureAwait(false);
        } 
        catch(OperationCanceledException)
        {
            //TODO
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

        switch(ev.Kind)
        {
            case ChangeKind.EnsureDir:
                Directory.CreateDirectory(destFullPath);
                break;
            case ChangeKind.CopyFile:
                if(ev.SourceFullPath is null)
                {
                    throw new InvalidOperationException("Copyfile requires SourceFullPath");
                }
                
                string? destDir = Path.GetDirectoryName(destFullPath);
                if(!string.IsNullOrEmpty(destDir))
                {
                    Directory.CreateDirectory(destDir);
                }

                await CopyFileAsync(ev.SourceFullPath, destFullPath, ct).ConfigureAwait(false);
                break;
            case ChangeKind.DeleteFile:
                if(File.Exists(destFullPath))
                {
                    File.Delete(destFullPath);
                }
                break;
            case ChangeKind.DeleteDir:
                if(Directory.Exists(destFullPath))
                {
                    Directory.Delete(destFullPath, recursive:true);
                }
                break;
            case ChangeKind.CreateSymlink:
                if(ev.LinkTarget == null)
                {
                    throw new InvalidOperationException("Symlink requires LinkTarget");
                }
                string? parent = Path.GetDirectoryName(destFullPath);
                if(!string.IsNullOrEmpty(parent)) 
                {
                    Directory.CreateDirectory(parent);
                }
                if(File.Exists(destFullPath)) {
                    File.Delete(destFullPath);
                }
                else if(Directory.Exists(destFullPath)) {
                    Directory.Delete(destFullPath, recursive: true);
                }
                
                if(ev.IsDirectoryLink)
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

    private async Task CopyFileAsync(string source, string destination, CancellationToken ct)
    {
        await Sem.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 131072, options:FileOptions.Asynchronous | FileOptions.SequentialScan);
            await using var dst =  new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 131072, options: FileOptions.Asynchronous);
            await src.CopyToAsync(dst, bufferSize: 1024 * 128, cancellationToken: ct).ConfigureAwait(false);
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
}