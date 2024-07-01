
//"data source=tcp:darsydevserver.database.windows.net,1433;initial catalog=DevToolsDB;persist security info=True;User ID=DevDBUser;Password=D@rsyT00ls20@!;MultipleActiveResultSets=True;Connection Timeout=250;App=EntityFramework"
//"data source=tcp:darsydevserver.database.windows.net,1433;initial catalog=DevToolsDB;persist security info=True;connection timeout=250;app=EntityFramework"
//string connectionString = "data source=tcp:darsydevserver.database.windows.net,1433;initial catalog=DevToolsDB;persist security info=True;connection timeout=250;app=EntityFramework"; // Replace with your actual connection string

using System;
using System.Data;
using System.Data.SqlClient;

namespace Darsy_DBCleanUP
{
    public class Programs
    {
        public static void Main(string[] args)
        {
            try
            {
                CheckingForNullColumns();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
            }
        }
        public static void CheckingForNullColumns()
        {
            string connectionString = "Server=(LocalDb)\\MSSQLLocalDB;Database=DarsyTestDB;Trusted_Connection=True;";
            //string connectionString = "data source=tcp:darsydevserver.database.windows.net,1433;initial catalog=DevToolsDB;persist security info=True;User ID=DevDBUser;Password=D@rsyT00ls20@!;MultipleActiveResultSets=True;Connection Timeout=250;App=EntityFramework";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    Console.WriteLine("Connection successful!");
                    DataTable tablesSchema = connection.GetSchema("Tables");

                    foreach (DataRow table in tablesSchema.Rows)
                    {
                        string tableName = table["TABLE_NAME"].ToString();
                        Console.WriteLine($"Checking table: {tableName}");

                        CheckColumnsForNulls(connection, tableName);
                        Console.WriteLine(new string('-', 50));
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"SQL exception: {ex.Message}");
                }
                catch (InvalidOperationException ex)
                {
                    Console.WriteLine($"Invalid operation: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        Console.WriteLine("Connection closed.");
                    }
                }
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static void CheckColumnsForNulls(SqlConnection connection, string tableName)
        {
            try
            {
                DataTable columnsSchema = connection.GetSchema("Columns", new[] { null, null, tableName });

                foreach (DataRow column in columnsSchema.Rows)
                {
                    string columnName = column["COLUMN_NAME"].ToString();
                    CheckColumnForNulls(connection, tableName, columnName);
                    CheckForLastUpdateColumn(connection, tableName, columnName);
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"SQL exception while processing table {tableName}: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while processing table {tableName}: {ex.Message}");
            }
        }

        private static void CheckColumnForNulls(SqlConnection connection, string tableName, string columnName)
        {
            try
            {
                string query1 = $"SELECT COUNT(*) FROM [{tableName}] WHERE [{columnName}] IS NULL";
                string query2 = $"SELECT COUNT(*) FROM [{tableName}] WHERE [{columnName}] IS NOT NULL";
                using (SqlCommand command1 = new SqlCommand(query1, connection))
                using (SqlCommand command2 = new SqlCommand(query2, connection))
                {
                    int countNulls = Convert.ToInt32(command1.ExecuteScalar());
                    int countNotNulls = Convert.ToInt32(command2.ExecuteScalar());
                    int Totalrows = countNulls + countNotNulls;
                    if (countNulls > 0)
                    {
                        Console.WriteLine($"Table :'{tableName}', Column :'{columnName}', Total Column:'{countNulls + countNotNulls}',  NULL value(s): '{countNulls}'  NOT NULL value(s):'{countNotNulls}' .");
                    }

                    else if (countNulls== Totalrows) {

                        Console.WriteLine($"Table '{tableName}', '{columnName}' entire column NULL");
                    }

                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"SQL exception while checking nulls in {tableName}.{columnName}: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while checking nulls in {tableName}.{columnName}: {ex.Message}");
            }
        }

        private static void CheckForLastUpdateColumn(SqlConnection connection, string tableName, string columnName)
        {
            if (columnName.ToLower().Contains("last") && columnName.ToLower().Contains("update") && !columnName.ToLower().Contains("by"))
            {
                try
                {
                    string lastUpdateQuery = $"SELECT MAX([{columnName}]) FROM [{tableName}]";
                    string firstUpdateQuery = $"SELECT MIN([{columnName}]) FROM [{tableName}]";
                    using (SqlCommand lastUpdateCommand = new SqlCommand(lastUpdateQuery, connection))
                    using (SqlCommand firstUpdateCommand = new SqlCommand(firstUpdateQuery, connection))
                    {
                        object lastUpdateDate = lastUpdateCommand.ExecuteScalar();
                        object firstUpdateDate = firstUpdateCommand.ExecuteScalar();
                        if (lastUpdateDate != DBNull.Value)
                        {
                            Console.WriteLine($"Table '{tableName}' was first updated on: {firstUpdateDate}");
                            Console.WriteLine($"Table '{tableName}' was last updated on: {lastUpdateDate}");
                           
                        }
                        else
                        {
                            Console.WriteLine($"Table '{tableName}' has no updates recorded in column '{columnName}'.");
                        }
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"SQL exception while checking last update in {tableName}.{columnName}: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred while checking last update in {tableName}.{columnName}: {ex.Message}");
                }
            }
        }
    }
}


























/*
namespace TableDataAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "YourConnectionStringHere";
            string tableName = "YourTableNameHere";
            string logFilePath = "TableAnalysisLog.txt";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    Console.WriteLine("Connection successful!");

                    // Get the last updated date and first updated date
                    GetLastUpdatedDate(connection, tableName, logFilePath);
                    GetFirstUpdatedDate(connection, tableName, logFilePath);

                    // Check for null values in each column
                    CheckNullValues(connection, tableName, logFilePath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        Console.WriteLine("Connection closed.");
                    }
                }
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        static void GetLastUpdatedDate(SqlConnection connection, string tableName, string logFilePath)
        {
            try
            {
                string query = $"SELECT MAX(LastUpdatedDate) FROM {tableName}";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    object lastUpdatedDate = command.ExecuteScalar();
                    Console.WriteLine($"Last Updated Date for table '{tableName}': {lastUpdatedDate}");

                    LogToFile(logFilePath, $"Last Updated Date for table '{tableName}': {lastUpdatedDate}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting last updated date: {ex.Message}");
            }
        }

        static void GetFirstUpdatedDate(SqlConnection connection, string tableName, string logFilePath)
        {
            try
            {
                string query = $"SELECT MIN(LastUpdatedDate) FROM {tableName}";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    object firstUpdatedDate = command.ExecuteScalar();
                    Console.WriteLine($"First Updated Date for table '{tableName}': {firstUpdatedDate}");

                    LogToFile(logFilePath, $"First Updated Date for table '{tableName}': {firstUpdatedDate}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting first updated date: {ex.Message}");
            }
        }

        static void CheckNullValues(SqlConnection connection, string tableName, string logFilePath)
        {
            try
            {
                string query = $"SELECT * FROM {tableName}";
                using (SqlCommand command = new SqlCommand(query, connection))
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    DataTable schemaTable = reader.GetSchemaTable();
                    if (schemaTable != null)
                    {
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            string columnName = row["ColumnName"].ToString();
                            bool allowNulls = Convert.ToBoolean(row["AllowDBNull"]);

                            if (allowNulls)
                            {
                                int nullCount = 0;
                                while (reader.Read())
                                {
                                    if (reader[columnName] == DBNull.Value)
                                    {
                                        nullCount++;
                                    }
                                }

                                Console.WriteLine($"Column '{columnName}' in table '{tableName}' has {nullCount} null value(s)");
                                LogToFile(logFilePath, $"Column '{columnName}' in table '{tableName}' has {nullCount} null value(s)");
                            }

                         
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking null values: {ex.Message}");
            }
        }

        static void LogToFile(string filePath, string logMessage)
        {
            try
            {
                using (StreamWriter writer = new StreamWriter(filePath, true))
                {
                    writer.WriteLine($"{DateTime.Now}: {logMessage}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error writing to log file: {ex.Message}");
            }
        }
    }
}
*/