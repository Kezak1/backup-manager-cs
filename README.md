# Backup Manager
Interactive backup management system for Linux. The system allows creating multiple backups of selected local directories and keeps them in sync by monitoring filesystem changes using C# (tasks, `FileSystemWatcher`, channels).

**Note:** The same project was also implemented in C:  
https://github.com/Kezak1/backup-manager

## Setup
1. **Requirements:**
   - Linux preferred; Windows/macOS may work, but symlink behavior may differ.
   - .NET SDK 9.0
2. **Run:**

   From the repository root:
   ```bash
   dotnet run --project backup
   ```
   Or from the project directory:
   ```bash
   dotnet run
   ```

## Behaviour
After running the program, you should see something like this in your terminal:
```
Welcome to the backup system!
USAGE:
- adding backup: add <source path> <target path 1> <target path 2> ... <target path n>, n >= 1
- ending backup: end <source path> <target path 1> <target path 2> ... <target path n>, n >= 1
- restore source from target: restore <source path> <target path>
- listing currently added backups: list
- exiting the program: exit
>
```

Now you can add a directory that you want to backup, e.g.:
```
add <source> <target 1> <target 2> ... <target n>
```

After adding `<source>` and creating the backup in `<target i>`, everything that happens in the `<source>` directory will be *mirrored* to the `<target i>`.  

**Supported operations:**
- creating files / directories / symlinks in `<source>`
- deleting files / directories / symlinks in `<source>`
- renaming files / directories / symlinks in `<source>`
- moving files / directories / symlinks in `<source>`

Files in `<target i>` should be exact copies of the corresponding files in `<source>`, but some metadata may be missing. The same applies to directories.  
If a symlink’s `LinkTarget` is an absolute path, the `LinkTarget` in the `<target i>` symlink will be rewritten to the corresponding absolute path with the `<target i>` path as a prefix.

**Undefined behaviour:**
- making any changes directly in `<target i>`
- moving or deleting `<source>` before calling `end` on the session with `<source>`
- and probably more

So it is advisable **not** to perform these actions.

To check currently watched backups:
```
list
```

To stop watching a backup (`<source>` -> `<target i>`), enter:
```
end <source> <target 1> <target 2> ... <target n>
```

To restore a `<source>` from a `<target>`, e.g.:
```
restore <source> <target>
```

Running this command will end the session for `<source>` and make `<source>` identical to `<target>`.

To close the program:
```
exit
```

**Note:** of course `<source>` and `<target i>` are placeholders representing directory paths.
