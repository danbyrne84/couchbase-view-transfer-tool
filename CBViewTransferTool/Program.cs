using System;
using System.Linq;
using Newtonsoft.Json.Serialization;

namespace CBViewTransferTool
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Couchbase View Transfer Tool");

            var tool = new TransferTool();
            var cmd = args.Any() ? args[0] : null;

            switch (cmd)
            {
                case "transfer":
                    tool.Transfer(args[1], args[2], args[3], args[4], args[5], args[6]);
                    break;
                case "backup":
                    tool.Backup(args[1], args[2], args[3]);
                    break;
                case "restore":
                    tool.Restore(args[1], args[2], args[3], args[4], args[5]);
                    break;
                default:
                    Console.WriteLine("Unknown option \"{0}\" specified:", cmd);
                    ShowUsage();
                    return;
            }

            Console.ReadLine();
            Console.ReadLine();
        }

        static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine();
            Console.WriteLine("BACKUP:");
            Console.WriteLine("backup nodeUrl bucket backupPath");
            Console.WriteLine();
            Console.WriteLine("RESTORE:");
            Console.WriteLine("restore backupPath nodeUrl bucket username password");
            Console.WriteLine();
            Console.WriteLine("TRANSFER:");
            Console.WriteLine("transfer sourceNodeUrl srcBucket targetNode targetBucket targetUsername targetPassword");
        }
    }
}
