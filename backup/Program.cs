using System;
using System.Text;

namespace backup;

class Program
{
    static void Usage(bool incorrectInput, string msg)
    {
        if(incorrectInput)
        {
            Console.Error.WriteLine($"Incorrect input: {msg}");
        }
        Console.Error.Write("""
        USAGE:
        - adding backup: add <source path> <target path 1> <target path 2> ... <target path n>, n >= 1
        - ending backup: end <source path> <target path 1> <target path 2> ... <target path n>, n >= 1
        - restore source from target: restore <source path> <target path>
        - listing currect added backups: list
        - exiting the program: exit
        """);   
    }

    private static void Flush(List<string> tokens, StringBuilder sb)
    {
        if(sb.Length == 0)
        {
            return;
        }
        tokens.Add(sb.ToString());
        sb.Clear();
    }

    static List<string> Tokenize(string? line)
    {
        var tokens = new List<string>();
        if(string.IsNullOrWhiteSpace(line))
        {
            return tokens;
        }

        var sb = new StringBuilder();
        
        int state = 0;
        bool atTokenBoundary = true;

        for(int i = 0; i < line.Length; i++)
        {
            char c = line[i];

            if(state == 0)
            {
                if(char.IsWhiteSpace(c))
                {
                    Flush(tokens, sb);
                    atTokenBoundary = true;
                    continue;
                }

                if(c == '#' && atTokenBoundary)
                {
                    break;
                }

                atTokenBoundary = false;

                if(c == '\'')
                {
                    state = 1;
                    continue;
                }
                if(c == '"')
                {
                    state = 2;
                    continue;
                }
                if(c == '\\')
                {
                    if(i + 1 < line.Length)
                    {
                        sb.Append(line[i + 1]);
                        i++;
                    } 
                    else
                    {
                        sb.Append('\\');    
                    }
                    continue;
                } 
                sb.Append(c);
            } 
            else if(state == 1)
            {
                if(c == '\'')
                {
                    state = 0;
                    continue;
                }
                sb.Append(c);
            } 
            else
            {
                if(c == '"')
                {
                    state = 0;
                    continue;
                }

                if(c == '\\')
                {
                    if(i + 1 >= line.Length)
                    {
                        sb.Append('\\');
                        continue;
                    }

                    char next = line[i + 1];

                    if(next == '\\' || next == '$' || next == '`' || next == '"')
                    {
                        sb.Append(next);
                        i++;
                        continue;
                    }
                    if(next == '\n')
                    {
                        i++;
                        continue;
                    }

                    sb.Append('\\');
                    continue;
                }
                sb.Append(c);
            }
        } 

        if (state != 0) {
            Usage(true, "Unclosed command line");
            tokens.Clear();
            return tokens;
        }

        Flush(tokens, sb);
        return tokens;
    }

    static void Main()
    {
        Console.WriteLine("Welcome to the backup system!");
        while(true)
        {
            string? line = Console.ReadLine();
            if(line == null)
            {
                Usage(true, "Input is null");
            }

            var tokens = Tokenize(line);
            int i = 0;
            foreach(var s in tokens)
            {
                i++;
                Console.WriteLine($"{i}:{s}");
            }

            break;
        }
        
    }
}