# Backup Manager
Interactive backup management system for Linux. The system allows creating multiple backups of selected local directories and keeps them in sync by monitoring filesystem changes using C# (tasks, Watcher, Channel). 

**Note:** Done same project in C https://github.com/Kezak1/backup-manager

## Setup
1. Requirements:
    - Linux preferable; Windows/macOS may work, but symlink behaviour may differ.
    - .NET SDK 9.0
2. Run:

    from the repo root:
    ```bash
    dotnet run --project backup
    ```
    or from the project dir:
    ```bash
    dotnet run
    ```
## Behaviour
After running the program you should get in your terminal something like this:
```
Welcome to the backup system!
USAGE:
- adding backup: add <source path> <target path 1> <target path 2> ... <target path n>, n >= 1
- ending backup: end <source path> <target path 1> <target path 2> ... <target path n>, n >= 1
- restore source from target: restore <source path> <target path>
- listing currect added backups: list
- exiting the program: exit
>
```

now you can add the directory that you would to do backup, e.g.:
```
add <source> <target 1> <target 2> ... <target n>
```

after adding the \<source> and creating backup in \<target>, everything that happends in \<source> directory will be "mirrored" to the \<target>. Supported operations: 
- creating files/directories/symlinks in \<source>
- deleting files/directories/symlinks in \<source>
- renaming files/directories/symlinks in \<source>
- moving files/directories/symlinks in \<source>

Files keep should be exact copy of the coresponding file in \<source>, but some metadata could be missing. Same with directories. If symlink's `TargetLink` is a full path, the `TargetLink` would be changed in the \<target>'s symlink to correspoding full path (with prefix of the \<target>'s full path).

Undefined behaviour:
- doing any changes in \<target>
- moving/deleting \<source> before calling `end` on session with <source>
- and probably more idk

so its advisable to not do those actions.

To check currenctly watched  backups:
```
list
```

To stop watching a backup: \<source> -> \<target> enter:
```
end <source> <target 1> <target 2> ... <target n>
``` 

To restore a \<source> with some \<target>, e.g.:
```
restore <source> <target>
```
Running this command will result in ending a session for \<source>, and it will basically make \<source> same as \<target>

And to close program:
```
exit
```