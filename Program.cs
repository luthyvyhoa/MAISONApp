using FluentFTP;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
namespace MAISONApp
{
    public static class Program
    {
        private static void Main(string[] args)
        {
            try
            {
                int CmdTimeout = Convert.ToInt32(ConfigurationManager.AppSettings["CmdTimeout"]);
                if (args == null || args.Length == 0)
                {
                    Console.Write("Enter Job: ");
                    args = new string[10];
                    args[0] = Console.ReadLine().ToString();
                }
                if (args != null && args.Length > 0)
                {
                    int job = Convert.ToInt32(args[0]);
                    new ThreadJob(job, CmdTimeout, 1);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }
    }
}