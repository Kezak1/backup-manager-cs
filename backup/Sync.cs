namespace backup;

public static class Sync
{
    public static async Task RunInitialSyncAsync(string sourceRoot, TargetWorker worker, CancellationToken ct)
    {
        await DfsAsync(sourceRoot, sourceRoot, worker, ct);
    }

    static async Task DfsAsync(string sourceRoot, string curPath, TargetWorker worker, CancellationToken ct)
    {
        foreach(var entry in Directory.EnumerateFileSystemEntries(curPath))
        {
            ct.ThrowIfCancellationRequested();

            FileSystemInfo fsi = Directory.Exists(entry) ? new DirectoryInfo(entry) : new FileInfo(entry);
            var rel = Path.GetRelativePath(sourceRoot, entry);


            if (IsSymlink(fsi))
            {
                if(fsi.LinkTarget is null)
                {
                    throw new InvalidOperationException("LinkTarget is null unexpectedly");
                    
                }
                var newLinkTarget = RewriteLinkTarget(fsi.LinkTarget, sourceRoot, worker.TargetRoot);
                bool isDirLink = (fsi.Attributes & FileAttributes.Directory) != 0;

                await worker.PushAsync(new ChangeEvent(ChangeKind.CreateSymlink, rel, LinkTarget: newLinkTarget, IsDirectoryLink: isDirLink), ct);
                continue;
            }

            if (Directory.Exists(entry))
            {
                await worker.PushAsync(new ChangeEvent(ChangeKind.EnsureDir, rel), ct);
                await DfsAsync(sourceRoot, entry, worker, ct);
            }
            else
            {
                await worker.PushAsync(new ChangeEvent(ChangeKind.CopyFile, rel, SourceFullPath: entry), ct);
            }
        }
    }

    static string RewriteLinkTarget(string link, string sourceRoot, string targetRoot)
    {
        if(!Path.IsPathRooted(link))
        {
            return link;
        }

        string full = Path.GetFullPath(link);
        string src = Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));

        if(!full.StartsWith(src + Path.DirectorySeparatorChar, StringComparison.Ordinal) && full != src)
        {
            return link;
        }

        string rel = Path.GetRelativePath(src, full);
        return Path.Combine(targetRoot, rel);
    }

    static bool IsSymlink(FileSystemInfo fsi)
    {
        return fsi.LinkTarget != null;
    }
}