namespace backup;

public class BackupSession {
    public string Source { get; }
    public HashSet<string> Targets { get; }

    public BackupSession()
    {
        Source = "";
        Targets = new();
    }

    public BackupSession(string source, IEnumerable<string> targets)
    {
        Source = source;
        Targets = new(targets, StringComparer.Ordinal);
    }
}

public class BackupManager
{
    Dictionary<string, BackupSession> sessions;

    public BackupManager()
    {
        sessions = new();
    }

    public void Add(string source, List<string> targets)
    {
        throw new NotImplementedException();
    }

    public void End(string source, List<string> targets)
    {
        throw new NotImplementedException();
    }

    public void List()
    {
        throw new NotImplementedException();
    }

    public void Restore(string source, string target)
    {
        throw new NotImplementedException();
    }

    public async Task StopAllAsync() {
        throw new NotImplementedException();
    }
}