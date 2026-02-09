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
    private Dictionary<string, BackupSession> Sessions = new(StringComparer.Ordinal);
    private object Gate = new();

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
        
        foreach(var t in ts)
        {
            if(PathEquals(t, src))
            {
                Console.Error.WriteLine("target cannot be equal to source");
                return;
            }
            if(IsSubPathOf(t, src))
            {
                Console.Error.WriteLine($"target '{t}' cannot be inside source '{src}");
                return;
            }
        }

        List<string> candidates;

        lock(Gate)
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
        foreach(var t in candidates)
        {
            if(!EnsureEmptyDirectory(t, out var err))
            {
                Console.Error.WriteLine(err);
                continue;
            }
            accepted.Add(t);
        }

        if(accepted.Count == 0) return;
        
        List<(string, TargetWorker)> toStart = [];
        lock(Gate)
        {
            if(!Sessions.TryGetValue(src, out var session))
            {
                session = new BackupSession(src);
                Sessions.Add(src, session);
            }

            foreach(var t in accepted)
            {
                if(session.Workers.ContainsKey(t)) continue;
                var w = new TargetWorker(src, t);
                session.Workers.Add(t, w);
                toStart.Add((t, w));
            }
        }

        foreach(var (t, w) in toStart)
        {
            _ = Task.Run(async () =>
            {
                try { 
                    await Sync.RunInitialSyncAsync(src, w, CancellationToken.None); 
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"initial sync failed for target '{t}': {e.Message}");
                    await w.DisposeAsync();
                    RemoveTargetWorker(src, t);
                }
            });
        }

        /*
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
                    Console.Error.WriteLine($"initial sync failed for target '{t}': {e.Message}");
                    await w.DisposeAsync();
                    RemoveTargetWorker(src, t);
                }
            });
            
        }
        */
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

        lock(Gate)
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
                Console.WriteLine($"session for source '{src}' ended");
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

        lock (Gate)
        {
            all = Sessions.Values.SelectMany(s => s.Workers.Values).ToList();
            Sessions.Clear();
        }

        foreach(var w in all)
        {
            try
            {
                await w.DisposeAsync().ConfigureAwait(false);
            }
            catch(Exception e)
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

        lock(Gate)
        {
            if(!Sessions.TryGetValue(src, out var s)) {
                return false;
            }
            if(!s.Workers.Remove(t, out var w)) {
                return false;
            }
            _ = w.DisposeAsync();
            if(s.Workers.Count == 0) {
                Sessions.Remove(src);
            }
            return true;
        }
    }
}