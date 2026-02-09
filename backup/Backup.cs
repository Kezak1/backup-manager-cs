using System.Data;
using System.Xml.Schema;

namespace backup;

public class BackupSession
{
    public string Source { get; }
    public Dictionary<string, TargetWorker> Workers { get; }
    public FileSystemWatcher? Watcher { get; set; }
    public int PendingInit { get; set; }

    public BackupSession(string source)
    {
        Source = source;
        Workers = new(StringComparer.Ordinal);
        Watcher = null;
        PendingInit = 0;
    }
}

public class BackupManager
{
    private Dictionary<string, BackupSession> Sessions = new(StringComparer.Ordinal);
    private object Gate = new();

    public void PushInitWatcher(string source)
    {
        FileSystemWatcher? watcherToStart = null;

        lock (Gate)
        {
            if (!Sessions.TryGetValue(source, out var s)) return;

            if (s.PendingInit > 0)
            {
                s.PendingInit--;
            }

            if (s.PendingInit == 0 && s.Watcher == null && s.Workers.Count > 0)
            {
                watcherToStart = BuildWatcher(s.Source);
                s.Watcher = watcherToStart;
            }
        }
    }

    public FileSystemWatcher BuildWatcher(string sourceRoot)
    {
        var w = new FileSystemWatcher(sourceRoot)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.LastWrite | NotifyFilters.CreationTime
        };

        w.Created += (_, e) => WatcherCreateOrUpdate(sourceRoot, e.FullPath);
        w.Changed += (_, e) => WatcherCreateOrUpdate(sourceRoot, e.FullPath);
        w.Deleted += (_, e) => WatcherDelete(sourceRoot, e.FullPath);
        w.Renamed += (_, e) => WatcherRename(sourceRoot, e.OldFullPath, e.FullPath);
        w.Error += (_, e) => Console.Error.WriteLine($"watcher error for '{sourceRoot}': {e.GetException()?.Message}");

        w.EnableRaisingEvents = true;
        return w;
    }

    private void WatcherCreateOrUpdate(string sourceRoot, string fullPath)
    {
        Task.Run(async () =>
        {
            if (!Directory.Exists(sourceRoot))
            {
                await StopSessionAsync(sourceRoot);
                return;
            }

            string rel;
            try
            {
                rel = Path.GetRelativePath(sourceRoot, fullPath);
            }
            catch
            {
                return;
            }

            if (rel.StartsWith("..")) return;

            try
            {
                var fi = new FileInfo(fullPath);
                if (fi.LinkTarget is string lt)
                {
                    bool isDirLink = (fi.Attributes & FileAttributes.Directory) != 0;
                    await BroadcastSymlinkAsync(sourceRoot, rel, lt, isDirLink).ConfigureAwait(false);
                    return;
                }
            }
            catch
            {
                return;
            }

            if (Directory.Exists(fullPath))
            {
                await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.EnsureDir, rel)).ConfigureAwait(false);
                return;
            }

            if (File.Exists(fullPath))
            {
                await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.CopyFile, rel, SourceFullPath: fullPath)).ConfigureAwait(false);
                return;
            }
        });
    }

    private void WatcherDelete(string sourceRoot, string fullPath)
    {
        Task.Run(async () =>
        {
            if (!Directory.Exists(sourceRoot))
            {
                await StopSessionAsync(sourceRoot);
                return;
            }

            string rel;
            try
            {
                rel = Path.GetRelativePath(sourceRoot, fullPath);
            }
            catch
            {
                return;
            }
            if (rel.StartsWith("..")) return;

            await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.DeleteFile, rel));
            await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.DeleteDir, rel));
        });
    }

    private void WatcherRename(string sourceRoot, string oldFullPath, string newFullPath)
    {
        Task.Run(async () =>
        {
            if (!Directory.Exists(sourceRoot))
            {
                await StopSessionAsync(sourceRoot);
                return;
            }

            string oldRel, newRel;
            try
            {
                oldRel = Path.GetRelativePath(sourceRoot, oldFullPath);
                newRel = Path.GetRelativePath(sourceRoot, newFullPath);
            }
            catch
            {
                return;
            }

            if (oldRel.StartsWith("..") || newRel.StartsWith("..")) return;

            await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.DeleteFile, oldRel));
            await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.DeleteDir, oldRel));


            if (Directory.Exists(newFullPath))
            {
                await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.EnsureDir, newRel));
                await BroadcastForSubtreeAsync(sourceRoot, newFullPath);
            }
            else if (File.Exists(newFullPath))
            {
                var fsi = new FileInfo(newFullPath);
                if (fsi.LinkTarget != null)
                {
                    bool isDirLink = (fsi.Attributes & FileAttributes.Directory) != 0;
                    await BroadcastSymlinkAsync(sourceRoot, newRel, fsi.LinkTarget, isDirLink);
                }
                else
                {
                    await BroadcastAsync(sourceRoot, new ChangeEvent(ChangeKind.CopyFile, newRel, SourceFullPath: newFullPath));
                }
            }
        });
    }

    private async Task BroadcastAsync(string sourceRoot, ChangeEvent ev)
    {
        List<TargetWorker> workers;
        lock (Gate)
        {
            if (!Sessions.TryGetValue(sourceRoot, out var s))
            {
                return;
            }

            workers = s.Workers.Values.ToList();
        }

        foreach (var w in workers)
        {
            try
            {
                await w.PushAsync(ev, CancellationToken.None);
            }
            catch { }
        }
    }

    private async Task BroadcastSymlinkAsync(string sourceRoot, string relative, string linkTarget, bool isDirLink)
    {
        List<TargetWorker> workers;
        lock (Gate)
        {
            if (!Sessions.TryGetValue(sourceRoot, out var s))
            {
                return;
            }
            workers = s.Workers.Values.ToList();
        }

        foreach (var w in workers)
        {
            string newLinkTarget = Sync.RewriteLinkTarget(linkTarget, sourceRoot, w.TargetRoot);
            ChangeEvent ev = new(ChangeKind.CreateSymlink, relative, LinkTarget: newLinkTarget, IsDirectoryLink: isDirLink);

            try
            {
                await w.PushAsync(ev, CancellationToken.None);
            }
            catch { }
        }
    }

    private async Task BroadcastForSubtreeAsync(string sourceRoot, string fullPath)
    {
        List<TargetWorker> workers;
        lock (Gate)
        {
            if (!Sessions.TryGetValue(sourceRoot, out var s)) return;
            workers = s.Workers.Values.ToList();
        }

        foreach (var w in workers)
        {
            try
            {
                await Sync.DfsAsync(sourceRoot, fullPath, w, CancellationToken.None);
            }
            catch { }
        }
    }

    private async Task StopSessionAsync(string sourceRoot)
    {
        List<TargetWorker> workers;
        FileSystemWatcher? watcher;

        lock (Gate)
        {
            if (!Sessions.Remove(sourceRoot, out var s)) return;
            workers = s.Workers.Values.ToList();
            watcher = s.Watcher;
        }

        watcher?.Dispose();

        foreach (var w in workers)
        {
            try
            {
                await w.DisposeAsync();
            }
            catch { }
        }
    }


    public void Add(string source, IEnumerable<string> targets)
    {
        string src = Normalize(source);

        if (File.Exists(src))
        {
            Console.Error.WriteLine($"source '{src}' is a file; expected a directory.");
            return;
        }
        if (!Directory.Exists(src))
        {
            Console.Error.WriteLine($"source directory '{src}' does not exist.");
            return;
        }

        var ts = targets
            .Where(t => !string.IsNullOrWhiteSpace(t))
            .Select(Normalize)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var t in ts)
        {
            if (PathEquals(t, src))
            {
                Console.Error.WriteLine("target cannot be equal to source");
                return;
            }
            if (IsSubPathOf(t, src))
            {
                Console.Error.WriteLine($"target '{t}' cannot be inside source '{src}");
                return;
            }
        }

        List<string> candidates;

        lock (Gate)
        {
            if (!Sessions.TryGetValue(src, out var session))
            {
                candidates = ts;
            }
            else
            {
                candidates = ts.Where(t => !session.Workers.ContainsKey(t)).ToList();
            }
        }

        List<string> accepted = [];
        foreach (var t in candidates)
        {
            if (!EnsureEmptyDirectory(t, out var err))
            {
                Console.Error.WriteLine(err);
                continue;
            }
            accepted.Add(t);
        }

        if (accepted.Count == 0) return;

        List<(string, TargetWorker)> toStart = [];
        lock (Gate)
        {
            if (!Sessions.TryGetValue(src, out var session))
            {
                session = new BackupSession(src);
                Sessions.Add(src, session);
            }

            foreach (var t in accepted)
            {
                if (session.Workers.ContainsKey(t)) continue;
                var w = new TargetWorker(src, t);
                session.Workers.Add(t, w);
                session.PendingInit++;
                toStart.Add((t, w));
            }
        }

        foreach (var (t, w) in toStart)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Sync.RunInitAsync(src, w, CancellationToken.None);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"initial sync failed for target '{t}': {e.Message}");
                    await w.DisposeAsync();
                    RemoveTargetWorker(src, t);
                }
                finally
                {
                    PushInitWatcher(src);
                }
            });
        }
    }

    public void End(string source, IEnumerable<string> targets)
    {
        EndAsync(source, targets).GetAwaiter().GetResult();
    }

    private async Task EndAsync(string source, IEnumerable<string> targets)
    {
        string src = Normalize(source);

        if (File.Exists(src))
        {
            Console.Error.WriteLine($"source '{src}' is a file; expected a directory");
            return;
        }
        if (!Directory.Exists(src))
        {
            Console.Error.WriteLine($"source directory '{src}' does not exist");
            return;
        }

        var ts = targets
            .Where(t => !string.IsNullOrWhiteSpace(t))
            .Select(Normalize)
            .ToList();

        List<TargetWorker> toStop = [];
        FileSystemWatcher? toDisposeWatcher = null;

        lock (Gate)
        {
            if (!Sessions.TryGetValue(src, out var session))
            {
                Console.Error.WriteLine($"no active session for source '{src}'.");
                return;
            }

            foreach (var t in ts)
            {
                if (session.Workers.Remove(t, out var worker))
                {
                    toStop.Add(worker);
                }
                else
                {
                    Console.Error.WriteLine($"target '{t}' is not registered for source '{src}'");
                }
            }

            if (session.Workers.Count == 0)
            {
                Sessions.Remove(src);
                toDisposeWatcher = session.Watcher;
                Console.WriteLine($"session for source '{src}' ended");
            }
        }

        toDisposeWatcher?.Dispose();

        foreach (var w in toStop)
        {
            try
            {
                await w.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"failed to stop worker for target '{w.TargetRoot}': {e.Message}");
            }
        }
    }

    public void List()
    {
        List<(string Source, List<string> Targets)> snapshot;
        lock (Gate)
        {
            snapshot = Sessions.Values
                .Select(s => (s.Source, s.Workers.Keys.OrderBy(t => t, StringComparer.Ordinal).ToList()))
                .OrderBy(x => x.Source, StringComparer.Ordinal)
                .ToList();
        }

        if (snapshot.Count == 0)
        {
            Console.WriteLine("no active backup sessions");
            return;
        }

        foreach (var (source, targets) in snapshot)
        {
            Console.WriteLine($"source: {source}");
            foreach (var t in targets)
            {
                Console.WriteLine($"  -> {t}");
            }
        }
    }

    public void Restore(string source, string target)
    {
        RestoreAsync(source, target).GetAwaiter().GetResult();
    }

    private async Task RestoreAsync(string source, string target)
    {
        string src = Normalize(source);
        string t = Normalize(target);

        await StopSessionAsync(src);

        await Sync.RestoreAsync(src, t, CancellationToken.None);
        Console.WriteLine($"restore completed: '{t}' -> '{src}'");
    }

    public async Task StopAllAsync()
    {
        List<TargetWorker> allWorkers;
        List<FileSystemWatcher> allWatchers;

        lock (Gate)
        {
            allWorkers = Sessions.Values.SelectMany(s => s.Workers.Values).ToList();
            allWatchers = Sessions.Values.Select(s => s.Watcher).Where(w => w != null).Cast<FileSystemWatcher>().ToList();
            Sessions.Clear();
        }

        foreach(var w in allWatchers)
        {
            try {
                w.Dispose();
            }
            catch { }   
        }

        foreach (var w in allWorkers)
        {
            try
            {
                await w.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"failed to stop watcher for target '{w.TargetRoot}: {e.Message}");
            }
        }
    }

    private static string Normalize(string path)
        => Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));

    private static bool PathEquals(string a, string b)
        => string.Equals(a, b, StringComparison.Ordinal);

    private static bool IsSubPathOf(string candidate, string basePath)
    {
        string baseWithSep = basePath + Path.DirectorySeparatorChar;
        string candWithSep = candidate + Path.DirectorySeparatorChar;

        return candWithSep.StartsWith(baseWithSep, StringComparison.Ordinal);
    }

    private static bool IsDirectoryEmpty(string dir)
    {
        return !Directory.EnumerateFileSystemEntries(dir).Any();
    }

    private static bool EnsureEmptyDirectory(string dir, out string error)
    {
        error = "";

        if (File.Exists(dir))
        {
            error = $"target '{dir}' exists and is a file.";
            return false;
        }

        if (!Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
            return true;
        }

        if (!IsDirectoryEmpty(dir))
        {
            error = $"target '{dir}' must be empty";
            return false;
        }

        return true;
    }

    private bool RemoveTargetWorker(string source, string target)
    {
        string src = Normalize(source);
        string t = Normalize(target);

        lock (Gate)
        {
            if (!Sessions.TryGetValue(src, out var s))
            {
                return false;
            }
            if (!s.Workers.Remove(t, out var w))
            {
                return false;
            }
            _ = w.DisposeAsync();
            if (s.Workers.Count == 0)
            {
                Sessions.Remove(src);
            }
            return true;
        }
    }
}