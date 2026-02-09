namespace backup;

public static class Sync
{
    public static async Task RunInitAsync(string sourceRoot, TargetWorker worker, CancellationToken ct)
    {
        await DfsAsync(sourceRoot, sourceRoot, worker, ct);
    }

    public static async Task DfsAsync(string sourceRoot, string curPath, TargetWorker worker, CancellationToken ct)
    {
        foreach(var entry in Directory.EnumerateFileSystemEntries(curPath))
        {
            ct.ThrowIfCancellationRequested();

            FileSystemInfo fsi = Directory.Exists(entry) ? new DirectoryInfo(entry) : new FileInfo(entry);
            var rel = Path.GetRelativePath(sourceRoot, entry);


            if(IsSymlink(fsi))
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

            if(Directory.Exists(entry))
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

    public static string RewriteLinkTarget(string link, string fromRoot, string toRoot)
    {
        if(!Path.IsPathRooted(link)) return link;

        string full = Path.GetFullPath(link);
        string from = Path.TrimEndingDirectorySeparator(Path.GetFullPath(fromRoot));

        if(!full.StartsWith(from + Path.DirectorySeparatorChar, StringComparison.Ordinal) && full != from)
            return link;

        string rel = Path.GetRelativePath(from, full);
        return Path.Combine(toRoot, rel);
    }

    public static bool IsSymlink(FileSystemInfo fsi)
    {
        return fsi.LinkTarget != null;
    }

    public static async Task RestoreAsync(string sourceRoot, string targetRoot, CancellationToken ct)
    {
        if (!Directory.Exists(targetRoot))
            throw new DirectoryNotFoundException($"target '{targetRoot}' does not exist");

        Directory.CreateDirectory(sourceRoot);

        var present = new HashSet<string>(StringComparer.Ordinal);

        await CopyFromTargetAsync(targetRoot, targetRoot, sourceRoot, present, ct);
        await DeleteMissingAsync(sourceRoot, sourceRoot, present, ct);
    }

    private static async Task CopyFromTargetAsync(string targetRoot, string curTarget, string sourceRoot, HashSet<string> present, CancellationToken ct)
    {
        foreach (var entry in Directory.EnumerateFileSystemEntries(curTarget))
        {
            ct.ThrowIfCancellationRequested();

            FileSystemInfo fsi = Directory.Exists(entry) ? new DirectoryInfo(entry) : new FileInfo(entry);
            string rel = Path.GetRelativePath(targetRoot, entry);
            present.Add(rel);

            string dst = Path.Combine(sourceRoot, rel);

            if(fsi.LinkTarget != null)
            {
                bool isDirLink = (fsi.Attributes & FileAttributes.Directory) != 0;
                string rewritten = RewriteLinkTarget(fsi.LinkTarget, targetRoot, sourceRoot);

                CreateSymlinkReplacing(dst, rewritten, isDirLink);
                continue;
            }

            if(fsi is DirectoryInfo)
            {
                if(File.Exists(dst)) File.Delete(dst);

                Directory.CreateDirectory(dst);
                await CopyFromTargetAsync(targetRoot, entry, sourceRoot, present, ct);
            }
            else
            {
                if(Directory.Exists(dst)) Directory.Delete(dst, recursive: true);

                string? parent = Path.GetDirectoryName(dst);
                if(!string.IsNullOrEmpty(parent)) Directory.CreateDirectory(parent);

                await CopyFileIfChangedAsync(entry, dst, ct);
            }
        }
    }

    private static async Task CopyFileIfChangedAsync(string source, string dest, CancellationToken ct)
    {
        var s = new FileInfo(source);
        if(File.Exists(dest))
        {
            var dInfo = new FileInfo(dest);
            if(dInfo.LinkTarget != null)
            {
                File.Delete(dest);
            }
            else
            {
                if(dInfo.Length == s.Length && dInfo.LastWriteTimeUtc == s.LastWriteTimeUtc) return;
            }
        }
        else if(Directory.Exists(dest))
        {
            Directory.Delete(dest, recursive: true);
        }

        await using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 131072, options: FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var dst = new FileStream(dest, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 131072, options: FileOptions.Asynchronous);

        await src.CopyToAsync(dst, 131072, ct);

        File.SetLastWriteTimeUtc(dest, s.LastWriteTimeUtc);
    }

    private static async Task DeleteMissingAsync(string sourceRoot, string curSource, HashSet<string> present, CancellationToken ct)
    {
        var entries = Directory.EnumerateFileSystemEntries(curSource).ToList();

        foreach(var entry in entries)
        {
            ct.ThrowIfCancellationRequested();

            FileSystemInfo fsi = Directory.Exists(entry) ? new DirectoryInfo(entry) : new FileInfo(entry);
            string rel = Path.GetRelativePath(sourceRoot, entry);

            if(rel == ".") continue;

            bool isSymlink = fsi.LinkTarget != null;

            if(!present.Contains(rel))
            {
                if(isSymlink || fsi is FileInfo)
                {
                    if(File.Exists(entry)) File.Delete(entry);
                }
                else
                {
                    if(Directory.Exists(entry)) Directory.Delete(entry, recursive: true);
                }
                continue;
            }

            if(!isSymlink && fsi is DirectoryInfo)
            {
                await DeleteMissingAsync(sourceRoot, entry, present, ct);
            }
        }
    }

    private static void CreateSymlinkReplacing(string path, string linkTarget, bool isDirLink)
    {
        string? parent = Path.GetDirectoryName(path);
        if(!string.IsNullOrEmpty(parent))
        {
            Directory.CreateDirectory(parent);
        }

        if(File.Exists(path)) 
        {
            File.Delete(path);
        }
        else if(Directory.Exists(path)) 
        {
            Directory.Delete(path, recursive: true);
        }

        if(isDirLink) 
        {
            Directory.CreateSymbolicLink(path, linkTarget);
        }
        else 
        {
            File.CreateSymbolicLink(path, linkTarget);
        }
    }
}