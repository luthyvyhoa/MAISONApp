using FluentFTP;
using Microsoft.VisualBasic.FileIO;
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
using System.Threading;
using WinSCP;
using SearchOption = System.IO.SearchOption;

namespace MAISONApp
{
    public class ThreadJob
    {
        public Thread thread;
        private int job;
        private int step;
        private int CmdTimeout;
        private int ProcessHandle;
        private string FolderBK;

        public ThreadJob(int Job, int cmdTimeout, int processHandle)
        {
            job = Job;
            CmdTimeout = cmdTimeout;
            ProcessHandle = processHandle;
            FolderBK = ConfigurationManager.AppSettings["FolderBK"];
            thread = new Thread(new ThreadStart(this.Run));
            thread.Start();
        }

        private void Run()
        {
            DataSet ds = Utility.ExecuteDataSet("IMEX_Step_GetList", job);

            if (ds != null && ds.Tables.Count > 0 && ds.Tables[0] != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    step = Convert.ToInt32(dr["Step"].ToString());
                    string taskType = dr["TaskType"].ToString();
                    string Ref = dr["Ref"].ToString();
                    DataSet dsJTP = Utility.ExecuteDataSet("IMEX_GetJobTrackingProcess", job, step);
                    if (dsJTP.Tables.Count == 0 || ProcessHandle == 1)
                    {
                        try
                        {
                            Console.WriteLine("Processing Job " + job + " step " + step);
                            switch (taskType)
                            {
                                case "COPY":
                                    Copy(job, step);
                                    break;

                                case "FTP":
                                    Ftp(job, step);
                                    break;

                                case "IMEX":
                                    DataFlow(job, step);
                                    break;

                                case "SQL":
                                    Sql(job, step);
                                    break;

                                case "E2FF":
                                    FlatFile(job, step);
                                    break;

                                case "CMD":
                                    Process p = Process.Start(Ref, string.Empty);
                                    p.WaitForExit();
                                    break;

                                case "ATM":
                                    ATM(job, step, Ref);
                                    break;
                            }
                            Utility.ExecuteNonQuery("IMEX_UpdateJobSuccess", job, step);
                            Console.WriteLine("Processing Job " + job + " step " + step + " success!!!");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("IMEXApp fail at job {0} - step {1} Error: {2}", job, step, e.Message);
                            Utility.ExecuteNonQuery("SMS_InsertMsg",
                                "84",
                                "0989989674",
                                string.Format("SGNA03: Job fail at job {0} - step {1}", job, step),
                                "SGNA03");
                            Utility.ExecuteNonQuery("EMS_Insert",
                                "Blue Ocean SGN/SGN/DKSH",
                                "Dong Dinh Nguyen/SGN/DKSH@DKSH",
                                "", "",
                                string.Format("IMEXApp fail at job {0} - step {1}", job, step),
                                e.Message.Replace('\'', ' '), "");
                            Utility.ExecuteNonQuery("IMEX_UpdateJobFail", job, step, e.Message.Replace('\'', ' '));
                        }
                    }
                }
            }
        }

        #region Copy

        private void Copy(int job, int step)
        {
            DataSet ds = Utility.ExecuteDataSet("IMEX_Copy_GetList", job, step);
            foreach (DataRow dr in ds.Tables[0].Rows)
            {
                Copy(dr["Source"].ToString(), dr["Dest"].ToString());
            }
        }

        private void Copy(string source, string dest)
        {
            string srcFolder = Path.GetDirectoryName(source);
            string dstFolder = Path.GetDirectoryName(dest);
            string dstName = Path.GetFileNameWithoutExtension(dest);
            string dstExt = Path.GetExtension(dest);
            string srcFileName = Path.GetFileName(source);

            string[] files = Directory.GetFiles(srcFolder, srcFileName, SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                string srcName = Path.GetFileNameWithoutExtension(file);
                string srcExt = Path.GetExtension(file);
                File.Copy(file, Path.Combine(dstFolder
                    , (dstName == "*" ? srcName : dstName)
                    + (dstExt == ".*" ? srcExt : dstExt)), true);
            }
        }

        #endregion Copy

        #region FTP

        private void Ftp(int job, int step)
        {
            DataSet ds = Utility.ExecuteDataSet("IMEX_Ftp_GetList", job, step);

            foreach (DataRow reader in ds.Tables[0].Rows)
            {
                if (string.IsNullOrEmpty(reader["Fingerprint"].ToString()))
                {
                    Ftp(reader["Url"].ToString(), reader["UserName"].ToString()
                        , reader["Password"].ToString(), reader["FtpPath"].ToString()
                            , reader["LocalPath"].ToString(), Convert.ToBoolean(reader["IsUpload"])
                                , Convert.ToBoolean(reader["IsAllFile"]), Convert.ToBoolean(reader["IsDelete"]));
                }
                else
                {
                    Sftp(reader["Url"].ToString(), reader["UserName"].ToString()
                        , reader["Password"].ToString(), reader["FtpPath"].ToString()
                            , reader["LocalPath"].ToString(), Convert.ToBoolean(reader["IsUpload"])
                                , reader["Fingerprint"].ToString(), Convert.ToInt32(reader["Port"].ToString())
                                    , Convert.ToBoolean(reader["IsAllFile"]), Convert.ToBoolean(reader["IsDelete"]));
                }
            }
        }

        private void Ftp(string url, string userName, string password, string ftpPath, string localPath, bool isUpload, bool IsAllFile, bool IsDelete)
        {
            FtpClient client = new FtpClient(url);
            client.Credentials = new NetworkCredential(userName, password);
            client.Connect();

            string folderBKSuccess = FolderBK + "Success";
            string folderBKFail = FolderBK + "Fail";
            if (!Directory.Exists(folderBKSuccess))
                Directory.CreateDirectory(folderBKSuccess);
            if (!Directory.Exists(folderBKFail))
                Directory.CreateDirectory(folderBKFail);
            string ftpPathNew = ftpPath;

            if (IsAllFile)
            {
                List<string> lstFile = Directory.GetFiles(localPath,"*.*", SearchOption.AllDirectories).ToList();
                foreach(string file in lstFile)
                {
                    try
                    {
                        FileInfo fi = new FileInfo(file);
                        ftpPathNew = ftpPath + fi.Name;
                        FileStream fs = new FileStream(file, FileMode.OpenOrCreate);
                        if (isUpload)
                        {
                            client.RetryAttempts = 3;
                            client.Upload(fs, ftpPathNew, FtpRemoteExists.Overwrite);
                            fs.Close();

                            //delete and bk file
                            if (IsDelete) File.Move(file, folderBKSuccess + "\\" + Path.GetFileName(file), true);
                            else File.Copy(file, folderBKSuccess + "\\" + Path.GetFileName(file), true);
                        }
                        else
                        {
                            MemoryStream stream = new MemoryStream();
                            if (client.FileExists(ftpPathNew))
                            {
                                client.Download(stream, ftpPathNew);
                            }
                            using (BinaryReader reader = new BinaryReader(stream))
                            {
                                using (BinaryWriter writer = new BinaryWriter(File.OpenWrite(file)))
                                   {
                                    Transfer(reader, writer);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("IMEXApp fail at job {0} - step {1} Error: {2}", job, step, e.Message);
                        Utility.ExecuteNonQuery("SMS_InsertMsg",
                            "84",
                            "0989989674",
                            string.Format("SGNA03: Job fail at job {0} - step {1}", job, step),
                            "SGNA03");
                        Utility.ExecuteNonQuery("EMS_Insert",
                            "Blue Ocean SGN/SGN/DKSH",
                            "Dong Dinh Nguyen/SGN/DKSH@DKSH",
                            "", "",
                            string.Format("IMEXApp fail at job {0} - step {1}", job, step),
                            e.Message.Replace('\'', ' '), "");
                    }
                }
            }
            else
            {
                try
                {
                    FileStream fs = new FileStream(localPath, FileMode.OpenOrCreate);
                    if (isUpload)
                    {
                        client.RetryAttempts = 3;
                        client.Upload(fs, ftpPathNew, FtpRemoteExists.Overwrite);
                        fs.Close();

                        //delete and bk file
                        if (IsDelete) File.Move(localPath, folderBKSuccess + "\\" + Path.GetFileName(localPath), true);
                        else File.Copy(localPath, folderBKSuccess + "\\" + Path.GetFileName(localPath), true);
                    }
                    else
                    {
                        MemoryStream stream = new MemoryStream();
                        if (client.FileExists(ftpPathNew))
                        {
                            client.Download(stream, ftpPathNew);
                        }
                        using (BinaryReader reader = new BinaryReader(stream))
                        {
                            using (BinaryWriter writer = new BinaryWriter(File.OpenWrite(localPath)))
                            {
                                Transfer(reader, writer);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("IMEXApp fail at job {0} - step {1} Error: {2}", job, step, e.Message);
                    Utility.ExecuteNonQuery("SMS_InsertMsg",
                        "84",
                        "0989989674",
                        string.Format("SGNA03: Job fail at job {0} - step {1}", job, step),
                        "SGNA03");
                    Utility.ExecuteNonQuery("EMS_Insert",
                        "Blue Ocean SGN/SGN/DKSH",
                        "Dong Dinh Nguyen/SGN/DKSH@DKSH",
                        "", "",
                        string.Format("IMEXApp fail at job {0} - step {1}", job, step),
                        e.Message.Replace('\'', ' '), "");
                }
            }
        }

        private void Sftp(string url, string userName, string password, string ftpPath, string localPath, bool isUpload, string Fingerprint, int port, bool IsAllFile, bool IsDelete)
        {
            SessionOptions sessionOptions = new SessionOptions
            {
                Protocol = Protocol.Sftp,
                HostName = url,
                UserName = userName,
                Password = password,
                SshHostKeyFingerprint = Fingerprint,
                PortNumber = port
            };

            string folderBKSuccess = FolderBK + "Success";
            string folderBKFail = FolderBK + "Fail";
            if (!Directory.Exists(folderBKSuccess))
                Directory.CreateDirectory(folderBKSuccess);
            if (!Directory.Exists(folderBKFail))
                Directory.CreateDirectory(folderBKFail);
            string ftpPathNew = ftpPath;

            using (Session session = new Session())
            {
                // Connect
                session.Open(sessionOptions);
                if (IsAllFile)
                {
                    List<string> lstFile = Directory.GetFiles(localPath, "*.*", SearchOption.AllDirectories).ToList();
                    foreach (string file in lstFile)
                    {
                        try
                        {
                            FileInfo fi = new FileInfo(file);
                            ftpPathNew = ftpPath + fi.Name;
                            if (isUpload)
                            {
                                session.PutFiles(file, ftpPathNew);

                                //delete and bk file
                                if (IsDelete) File.Move(file, folderBKSuccess + "\\" + Path.GetFileName(file), true);
                                else File.Copy(file, folderBKSuccess + "\\" + Path.GetFileName(file), true);
                            }
                            else
                            {
                                session.GetFiles(ftpPathNew, file);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("IMEXApp fail at job {0} - step {1} Error: {2}", job, step, e.Message);
                            Utility.ExecuteNonQuery("SMS_InsertMsg",
                                "84",
                                "0989989674",
                                string.Format("SGNA03: Job fail at job {0} - step {1}", job, step),
                                "SGNA03");
                            Utility.ExecuteNonQuery("EMS_Insert",
                                "Blue Ocean SGN/SGN/DKSH",
                                "Dong Dinh Nguyen/SGN/DKSH@DKSH",
                                "", "",
                                string.Format("IMEXApp fail at job {0} - step {1}", job, step),
                                e.Message.Replace('\'', ' '), "");
                        }
                    }
                }
                else
                {
                    try
                    {
                        if (isUpload)
                        {
                            session.PutFiles(localPath, ftpPathNew);

                            //delete and bk file
                            if (IsDelete) File.Move(localPath, folderBKSuccess + "\\" + Path.GetFileName(localPath), true);
                            else File.Copy(localPath, folderBKSuccess + "\\" + Path.GetFileName(localPath), true);
                        }
                        else
                        {
                            session.GetFiles(ftpPathNew, localPath);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("IMEXApp fail at job {0} - step {1} Error: {2}", job, step, e.Message);
                        Utility.ExecuteNonQuery("SMS_InsertMsg",
                            "84",
                            "0989989674",
                            string.Format("SGNA03: Job fail at job {0} - step {1}", job, step),
                            "SGNA03");
                        Utility.ExecuteNonQuery("EMS_Insert",
                            "Blue Ocean SGN/SGN/DKSH",
                            "Dong Dinh Nguyen/SGN/DKSH@DKSH",
                            "", "",
                            string.Format("IMEXApp fail at job {0} - step {1}", job, step),
                            e.Message.Replace('\'', ' '), "");
                    }
                }
            }
        }

        private void Transfer(BinaryReader reader, BinaryWriter writer)
        {
            byte[] buffer = new byte[1024];
            int count = 1024;
            while (true)
            {
                count = reader.Read(buffer, 0, 1024);
                if (count > 0)
                    writer.Write(buffer, 0, count);
                else
                    break;
            }
        }

        #endregion FTP

        #region IMEX

        private void DataFlow(int job, int step)
        {
            DataSet ds = Utility.ExecuteDataSet("IMEX_DataFlow_GetList", job, step);
            foreach (DataRow reader in ds.Tables[0].Rows)
            {
                List<string> lstSourceSQL = ReplaceParameter(reader["SourceSQL"].ToString());
                foreach (string strSourceSQL in lstSourceSQL)
                {
                    if ("Text".Equals(Convert.ToString(reader["SourceConn"])))
                    {
                        DataFlow(Convert.ToString(reader["SourceData"]), strSourceSQL
                            , Convert.ToString(reader["DestConn"]), Convert.ToString(reader["DestData"])
                            , Convert.ToString(reader["DestSQL"]), Convert.ToInt32(reader["NumOfCols"]));
                    }
                    else
                    {
                        DataFlow(Convert.ToString(reader["SourceConn"]), Convert.ToString(reader["SourceData"])
                            , strSourceSQL, Convert.ToString(reader["DestConn"])
                            , Convert.ToString(reader["DestData"]), Convert.ToString(reader["DestSQL"])
                            , Convert.ToInt32(reader["NumOfCols"]));
                    }
                }
            }
        }

        private void DataFlow(string sourceConn, string sourceData, string sourceSQL, string destConn
            , string destData, string destSQL, int numOfCols)
        {
            using (SqlConnection srcConn = new SqlConnection(string.Format(sourceConn, sourceData)))
            {
                SqlCommand srcCmd = new SqlCommand(sourceSQL, srcConn);
                srcCmd.CommandTimeout = CmdTimeout;
                srcConn.Open();

                using (SqlConnection dstConn = new SqlConnection(string.Format(destConn, destData)))
                {
                    SqlCommand dstCmd = new SqlCommand(destSQL, dstConn);
                    dstCmd.CommandTimeout = CmdTimeout;
                    dstConn.Open();

                    dstCmd.Prepare();
                    using (SqlDataReader reader = srcCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (int i = 0; i < numOfCols; i++)
                            {
                                dstCmd.Parameters.AddWithValue(ParameterName(i), reader.GetValue(i));
                            }

                            dstCmd.ExecuteNonQuery();
                        }
                    }

                    dstConn.Close();
                }

                srcConn.Close();
            }
        }

        private string ParameterName(int idx)
        {
            return "@C" + idx.ToString().PadLeft(4, '0');
        }

        private void DataFlow(string sourceData, string sourceSQL, string destConn
            , string destData, string destSQL, int numOfCols)
        {
            // Scan folder file
            // Copy file to Backup and delete file root
            string folderBKSuccess = FolderBK + "Success";
            string folderBKFail = FolderBK + "Fail";
            if (!Directory.Exists(folderBKSuccess))
                Directory.CreateDirectory(folderBKSuccess);
            if (!Directory.Exists(folderBKFail))
                Directory.CreateDirectory(folderBKFail);
            List<string> lstFile = Directory.GetFiles(sourceData, sourceSQL + "*.*", SearchOption.AllDirectories).ToList();
            foreach (string file in lstFile)
            {
                try
                {
                    // insert data to table tmp
                    DataTable dtSource = GetDataTabletFromCSVFile(file);
                    InsertDataIntoSQLServerUsingSQLBulkCopy(dtSource, destConn, destData);

                    //delete and bk file
                    File.Move(file, folderBKSuccess + "\\" + Path.GetFileName(file), true);
                }
                catch (Exception e)
                {
                    Console.WriteLine("IMEXApp fail at job {0} - step {1} Error: {2}", job, step, e.Message);
                    Utility.ExecuteNonQuery("SMS_InsertMsg",
                        "84",
                        "0989989674",
                        string.Format("SGNA03: Job fail at job {0} - step {1}", job, step),
                        "SGNA03");
                    Utility.ExecuteNonQuery("EMS_Insert",
                        "Blue Ocean SGN/SGN/DKSH",
                        "Dong Dinh Nguyen/SGN/DKSH@DKSH",
                        "", "",
                        string.Format("IMEXApp fail at job {0} - step {1}", job, step),
                        e.Message.Replace('\'', ' '), "");
                    File.Move(file, folderBKFail + "\\" + Path.GetFileName(file), true);
                }
            }
        }

        #endregion IMEX

        #region SQL

        private void Sql(int job, int step)
        {
            DataSet ds = Utility.ExecuteDataSet("IMEX_Sql_GetList", job, step);
            foreach (DataRow reader in ds.Tables[0].Rows)
            {
                List<string> lstSourceSQL = ReplaceParameter(reader["SourceSQL"].ToString());
                foreach (string strSourceSQL in lstSourceSQL)
                {
                    Sql(Convert.ToString(reader["SourceConn"]), Convert.ToString(reader["SourceData"]), strSourceSQL);
                }
            }
        }

        private void Sql(string sourceConn, string sourceData, string sourceSQL)
        {
            using (SqlConnection srcConn = new SqlConnection(string.Format(sourceConn, sourceData)))
            {
                SqlCommand srcCmd = new SqlCommand(sourceSQL, srcConn);
                srcCmd.CommandTimeout = CmdTimeout;
                srcConn.Open();
                srcCmd.ExecuteNonQuery();
                srcConn.Close();
            }
        }

        #endregion SQL

        #region E2FF

        private void FlatFile(int job, int step)
        {
            DataSet ds = Utility.ExecuteDataSet("IMEX_FlatFile_GetList", job, step);
            foreach (DataRow reader in ds.Tables[0].Rows)
            {
                string strExten = Convert.ToString(reader["Extensions"]);
                List<string> lstSourceSQL = ReplaceParameter(reader["SourceSQL"].ToString());
                int i = 1;
                foreach (string strSourceSQL in lstSourceSQL)
                {
                    string DestFile = Convert.ToString(reader["DestFile"]) + "." + strExten;
                    if (i > 1) DestFile = DestFile.Replace(".", "_" + i + ".");
                    if (strExten.ToUpper() == "CSV" || strExten.ToUpper() == "TXT")
                    {
                        FlatFileCSV(Convert.ToString(reader["SourceConn"]), Convert.ToString(reader["SourceData"])
                            , strSourceSQL, DestFile, Convert.ToBoolean(reader["IsHeader"])
                                , string.IsNullOrEmpty(Convert.ToString(reader["FormatType"]))? "\t" : Convert.ToString(reader["FormatType"])
                                    , Convert.ToBoolean(reader["IsFileName"]));
                    }
                    else
                    {
                        FlatFileExcel(Convert.ToString(reader["SourceConn"]), Convert.ToString(reader["SourceData"])
                            , strSourceSQL, DestFile);
                    }
                    i++;
                }
            }
        }

        private void FlatFileCSV(string sourceConn, string sourceData, string sourceSQL, string destFile, bool IsHeader, string FormatType, bool IsFileName)
        {
            string dir = Path.GetDirectoryName(destFile);
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);
            string fileName = string.Empty;
            bool isNewName = false;
            using (SqlConnection srcConn = new SqlConnection(string.Format(sourceConn, sourceData)))
            {
                SqlCommand srcCmd = new SqlCommand(sourceSQL, srcConn);
                srcCmd.CommandTimeout = CmdTimeout;
                srcConn.Open();

                using (StreamWriter writer = new StreamWriter(File.Open(destFile, FileMode.Create)))
                {
                    using (SqlDataReader reader = srcCmd.ExecuteReader())
                    {
                        int i = 0;
                        List<string> list = new List<string>();
                        int fieldCount = reader.FieldCount;
                        for (; i < fieldCount; i++)
                        {
                            if (reader.GetName(i).ToUpper() == "@FILENAME")
                            {
                                isNewName = true;
                            }
                            list.Add(reader.GetName(i));
                        }
                        if (IsHeader)
                        {
                            writer.WriteLine(string.Join(FormatType, list.ToArray()));
                        }
                        while (reader.Read())
                        {
                            for (i = 0; i < fieldCount; i++)
                            {
                                list[i] = reader[i].ToString();
                            }
                            if (isNewName)
                            {
                                fileName = list[i - 1];
                                if (!IsFileName)
                                {
                                    list.Remove(list[i - 1]);
                                    fieldCount--;
                                }
                                isNewName = false;
                            }
                            writer.WriteLine(string.Join(FormatType, list.ToArray()));
                        }
                    }
                }
                srcConn.Close();
            }
            if (!string.IsNullOrEmpty(fileName))
            {
                FileInfo fi = new FileInfo(destFile);
                if (fi.Exists)
                {
                    fi.MoveTo(destFile.Replace(fi.Name, (fi.Name.Contains("Temp") ? "" : (Path.GetFileNameWithoutExtension(destFile))) + fileName), true);
                }
            }
        }

        private void FlatFileExcel(string sourceConn, string sourceData, string sourceSQL, string destFile)
        {
            string dir = Path.GetDirectoryName(destFile);
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);
            using (SqlConnection srcConn = new SqlConnection(string.Format(sourceConn, sourceData)))
            {
                SqlCommand srcCmd = new SqlCommand(sourceSQL, srcConn);
                srcCmd.CommandTimeout = CmdTimeout;
                srcConn.Open();

                using (FileStream stream = new FileStream(destFile, FileMode.Create, FileAccess.Write))
                {
                    IWorkbook wb = new XSSFWorkbook();
                    ISheet sheet = wb.CreateSheet();

                    XSSFCellStyle headerStyle1 = (XSSFCellStyle)wb.CreateCellStyle();
                    XSSFFont headerFont1 = (XSSFFont)wb.CreateFont();
                    headerFont1.Color = XSSFFont.DEFAULT_FONT_COLOR;
                    headerFont1.IsBold = true;
                    headerStyle1.SetFont(headerFont1);
                    headerStyle1.SetFillForegroundColor(new XSSFColor(Color.LightGreen));
                    headerStyle1.FillPattern = FillPattern.SolidForeground;
                    headerStyle1.BorderRight = BorderStyle.Thin;
                    headerStyle1.BorderLeft = BorderStyle.Thin;
                    headerStyle1.BorderBottom = BorderStyle.Thin;

                    XSSFCellStyle normalStyle = (XSSFCellStyle)wb.CreateCellStyle();
                    XSSFFont normalFont = (XSSFFont)wb.CreateFont();
                    normalFont.Color = XSSFFont.DEFAULT_FONT_COLOR;
                    normalStyle.SetFont(normalFont);
                    normalStyle.BorderRight = BorderStyle.Thin;
                    normalStyle.BorderLeft = BorderStyle.Thin;
                    normalStyle.BorderBottom = BorderStyle.Thin;
                    normalStyle.SetDataFormat(HSSFDataFormat.GetBuiltinFormat("#,##0.00"));

                    using (SqlDataReader reader = srcCmd.ExecuteReader())
                    {
                        int i = 0;
                        IRow row = sheet.CreateRow(0);
                        List<string> list = new List<string>();
                        for (; i < reader.FieldCount; i++)
                        {
                            ICell cell = row.CreateCell(i);
                            cell.SetCellValue(reader.GetName(i));
                            cell.CellStyle = headerStyle1;
                        }
                        i = 1;
                        while (reader.Read())
                        {
                            IRow rowLoop = sheet.CreateRow(i);
                            for (int j = 0; j < reader.FieldCount; j++)
                            {
                                ICell cell = rowLoop.CreateCell(j);
                                cell.SetCellValue(reader[j].ToString());
                                cell.CellStyle = normalStyle;
                            }
                            i++;
                        }
                        for (i = 0; i < reader.FieldCount; i++)
                        {
                            sheet.AutoSizeColumn(i);
                        }
                    }

                    wb.Write(stream);
                }

                srcConn.Close();
            }
        }

        #endregion E2FF

        #region ATM

        private void ATM(int job, int step, string Ref)
        {
            string atmId = string.Empty;
            string description = string.Format("IMEX Job: {0} - Step: {1}", job, step);

            DataSet ds = Utility.ExecuteDataSet("IMEX_ATM_GetList", job, step);
            foreach (DataRow reader in ds.Tables[0].Rows)
            {
                ATM(ref atmId, Convert.ToString(reader["Source"]), description);
            }

            Utility.ExecuteNonQuery(Ref, job, step, atmId);
        }

        private void ATM(ref string atmId, string source, string description)
        {
            string srcFolder = Path.GetDirectoryName(source);
            string srcFileName = Path.GetFileName(source);

            string[] files = Directory.GetFiles(srcFolder, srcFileName, SearchOption.TopDirectoryOnly);
            if (files.Length > 0)
            {
                //FileService.Attachment atm = new FileService.Attachment();
                //atmId = atm.Insert(description);
                foreach (string file in files)
                {
                    using (FileStream fs = File.Open(file, FileMode.Open, FileAccess.Read))
                    {
                        int len = Convert.ToInt32(fs.Length);
                        byte[] buffer = new byte[len];
                        fs.Read(buffer, 0, len);
                        //atm.Add(atmId, Path.GetFileName(file), "application/octet-stream", len, buffer);
                    }
                }
            }
        }

        #endregion ATM

        #region Function

        private List<string> ReplaceParameter(string input)
        {
            List<string> lstResult = new List<string>();
            DataSet ds = Utility.ExecuteDataSet("SELECT * FROM dbo.IMEX_Parameter");
            int x = 0;
            int y = 0;
            string[,] arrLstResult = new string[10, 100000];
            string[] arrString = input.Split(' ');
            List<string> lstCos = new List<string>();
            List<string> lstField = new List<string>();

            foreach (string strParam in arrString)
            {
                string strParamTrim = strParam.Trim('\'');
                foreach (string obj in strParamTrim.Split(','))
                {
                    if (obj.Substring(0, 1) == "@")
                    {
                        lstField.Add(obj.Trim());
                    }
                    else if (obj.Substring(0, 1) == "$")
                    {
                        lstCos.Add(obj.Trim());
                    }
                }
            }

            foreach (string strField in lstField.Distinct())
            {
                string strRep = ds.Tables[0].Select("Field = '" + strField + "'").FirstOrDefault()["Value"].ToString();
                input = input.Replace(strField, "'" + (strRep.ToUpper() == "GETDATE()" ? DateTime.Now.ToString("yyyy-MM-dd") : strRep) + "'");
            }

            foreach (string strCos in lstCos.Distinct())
            {
                if (x > 0)
                {
                    int z = 0;
                    for (int i = 0; i < y; i++)
                    {
                        List<DataRow> lstDR = ds.Tables[0].Select("Field = '" + strCos + "'").ToList();
                        foreach (DataRow dr in lstDR)
                        {
                            arrLstResult[x, z] = arrLstResult[x - 1, i].Replace(strCos, "'" + (dr["Value"].ToString().ToUpper() == "GETDATE()" ? DateTime.Now.Date.ToString() : dr["Value"].ToString()) + "'");
                            z++;
                        }
                    }
                    y = z;
                    x++;
                }
                else
                {
                    List<DataRow> lstDR = ds.Tables[0].Select("Field = '" + strCos + "'").ToList();
                    foreach (DataRow dr in lstDR)
                    {
                        arrLstResult[x, y] = input.Replace(strCos, "'" + (dr["Value"].ToString().ToUpper() == "GETDATE()" ? DateTime.Now.Date.ToString() : dr["Value"].ToString()) + "'");
                        y++;
                    }
                    x++;
                }
            }

            if (x > 0)
            {
                for (int i = 0; i < y; i++)
                {
                    lstResult.Add(arrLstResult[x - 1, i]);
                }
            }
            else
            {
                lstResult.Add(input);
            }

            return lstResult;
        }

        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "\t" });
                    csvReader.HasFieldsEnclosedInQuotes = false;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
                return csvData;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message);
            }
        }

        static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData, string SQLConnect, string dataTable)
        {
            try
            {
                using (SqlConnection dbConnection = new SqlConnection(SQLConnect))
                {
                    dbConnection.Open();
                    using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                    {
                        s.DestinationTableName = dataTable;
                        s.BulkCopyTimeout = 0;
                        foreach (var column in csvFileData.Columns)
                            s.ColumnMappings.Add(column.ToString(), column.ToString());
                        s.WriteToServer(csvFileData);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message);
            }
        }

        #endregion Function
    }
}