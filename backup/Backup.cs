namespace backup;

public class BackupSession {
    public string Source { get; }
    public HashSet<string> Targets { get; }

    public BackupSession(string source, IEnumerable<string> targets)
    {
        Source = source;
        Targets = new(targets, StringComparer.Ordinal);
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
            if(!EnsureEmptyDirectory(t, out var err))
            {
                Console.Error.WriteLine(err);
                return;
            }
        }

        lock (gate) {
            if(!sessions.TryGetValue(src, out var session))
            {
                session = new BackupSession(src, ts);
                sessions.Add(src, session);
                return;
            }

            foreach(var t in ts)
            {
                session.Targets.Add(t);
            }
        }
    }

    public void End(string source, IEnumerable<string> targets)
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

        lock(gate)
        {
            if (!sessions.TryGetValue(src, out var session))
            {
                Console.Error.WriteLine($"no active session for source '{src}'.");
                return;
            }

            foreach (var t in ts)
            {
                if (!session.Targets.Remove(t))
                {
                    Console.Error.WriteLine($"target '{t}' is not registered for source '{src}'");
                }
            }

            if (session.Targets.Count == 0)
            {
                sessions.Remove(src);
            }   
        }
    }

    public void List()
    {
        List<(string Source, List<string> Targets)> snapshot;
        lock (gate)
        {
            snapshot = sessions.Values
                .Select(s => (s.Source, s.Targets.OrderBy(t => t, StringComparer.Ordinal).ToList()))
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
        throw new NotImplementedException();
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
}