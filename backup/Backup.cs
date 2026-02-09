using System.Data;

namespace backup;

public class BackupSession {
    public string Source { get; }
    public Dictionary<string, TargetWorker> Workers { get; }

    public BackupSession(string source)
    {
        Source = source;
        Workers = new(StringComparer.Ordinal);
    }
}

public class BackupManager
{
    private Dictionary<string, BackupSession> sessions = new(StringComparer.Ordinal);
    private object gate = new();

    public void Add(string source, IEnumerable<string> targets)
    {
        string src = Normalize(source);

        if (File.Exists(src))
        {
            Console.Error.WriteLine($"Source '{src}' is a file; expected a directory.");
            return;
        }
        if (!Directory.Exists(src))
        {
            Console.Error.WriteLine($"Source directory '{src}' does not exist.");
            return;
        }

        var ts = targets
            .Where(t => !string.IsNullOrWhiteSpace(t))
            .Select(Normalize)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        
        foreach(var t in ts)
        {
            if(PathEquals(t, src))
            {
                Console.Error.WriteLine("Target cannot be equal to source");
                return;
            }
            if(IsSubPathOf(t, src))
            {
                Console.Error.WriteLine($"Target '{t}' cannot be inside source '{src}");
                return;
            }
        }

        List<string> newTargets = [];
        lock(gate)
        {
            if(!sessions.TryGetValue(src, out var session))
            {
                session = new BackupSession(src);
                sessions.Add(src, session);
            }

            foreach(var t in ts)
            {
                if(session.Workers.ContainsKey(t)) continue;
                newTargets.Add(t);
            }
        }

        foreach (var t in newTargets)
        {
            if (!EnsureEmptyDirectory(t, out var err))
            {
                Console.Error.WriteLine(err);
                continue;
            }

            TargetWorker w;

            lock(gate)
            {
                if(!sessions.TryGetValue(src, out var session))
                    return;

                if(session.Workers.ContainsKey(t))
                    continue;

                w = new TargetWorker(src, t);
                session.Workers.Add(t, w);
            }

            Task.Run(async () =>
            {
                try
                {
                    await Sync.RunInitialSyncAsync(src, w, CancellationToken.None);
                }
                catch(Exception e)
                {
                    Console.Error.WriteLine($"Initial sync failed for target '{t}': {e.Message}");
                    await w.DisposeAsync();
                    RemoveTargetWorker(src, t);
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
        bool removeSession = false;

        lock(gate)
        {
            if (!sessions.TryGetValue(src, out var session))
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
                sessions.Remove(src);
                removeSession = true;
            }
        }

        foreach(var w in toStop)
        {
            try
            {
                await w.DisposeAsync().ConfigureAwait(false);
            } 
            catch(Exception e)
            {
                Console.Error.WriteLine($"Failed to stop worker for target '{w.TargetRoot}': {e.Message}");    
            }
        }

        if(removeSession)
        {
            Console.WriteLine($"Session for source '{src}' ended");
        }
    }

    public void List()
    {
        List<(string Source, List<string> Targets)> snapshot;
        lock (gate)
        {
            snapshot = sessions.Values
                .Select(s => (s.Source, s.Workers.Keys.OrderBy(t => t, StringComparer.Ordinal).ToList()))
                .OrderBy(x => x.Source, StringComparer.Ordinal)
                .ToList();
        }

        if (snapshot.Count == 0)
        {
            Console.WriteLine("No active backup sessions.");
            return;
        }

        foreach (var (source, targets) in snapshot)
        {
            Console.WriteLine($"Source: {source}");
            foreach (var t in targets)
                Console.WriteLine($"  -> {t}");
        }
    }

    public void Restore(string source, string target)
    {
        throw new NotImplementedException();
    }

    public async Task StopAllAsync() 
    {
        List<TargetWorker> all;

        lock (gate)
        {
            all = sessions.Values.SelectMany(s => s.Workers.Values).ToList();
            sessions.Clear();
        }

        foreach(var w in all)
        {
            try
            {
                await w.DisposeAsync().ConfigureAwait(false);
            }
            catch(Exception e)
            {
                Console.Error.WriteLine($"Failed to stop watcher for target '{w.TargetRoot}: {e.Message}");
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

        lock(gate)
        {
            if(!sessions.TryGetValue(src, out var s)) {
                return false;
            }
            if(!s.Workers.Remove(t, out var w)) {
                return false;
            }
            _ = w.DisposeAsync();
            if(s.Workers.Count == 0) {
                sessions.Remove(src);
            }
            return true;
        }
    }
}