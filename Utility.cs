using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace MAISONApp
{
    public class Utility
    {
        private static string connectionString = ConfigurationManager.ConnectionStrings["IMEXDB"].ToString();

        public static DataSet ExecuteDataSet(string strQuerySQL, params object[] parameterValues)
        {
            DataSet ds = new DataSet();
            string strParam = string.Empty;
            foreach (object obj in parameterValues)
            {
                strParam += " " + obj + ",";
            }
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand(strQuerySQL + strParam.Trim(','), conn))
                {
                    cmd.CommandTimeout = 0;
                    cmd.CommandType = CommandType.Text;

                    using (var adapt = new SqlDataAdapter(cmd))
                    {
                        adapt.Fill(ds);
                    }
                }

                conn.Close();
            }
            return ds;
        }

        public static void ExecuteNonQuery(string strQuerySQL, params object[] parameterValues)
        {
            string strParam = string.Empty;
            foreach (object obj in parameterValues)
            {
                strParam += " '" + obj + "',";
            }
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand(strQuerySQL + strParam.Trim(','), conn))
                {
                    cmd.CommandTimeout = 0;
                    cmd.CommandType = CommandType.Text;

                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            }
        }
    }
}