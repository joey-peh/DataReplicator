using System.ComponentModel.DataAnnotations;
using System.Data;
using Microsoft.Data.SqlClient;
using static System.Formats.Asn1.AsnWriter;

bool replicateAllTablesIncludingRtTables = false;

Console.WriteLine("Replicate to local db? (Y/N)");
bool replicateToLocalDb = Console.ReadLine().Equals("Y", StringComparison.OrdinalIgnoreCase);

Console.WriteLine("Replicate all tables? If false, will only replicate tables specified in sourceToDestinationDict. (Y/N)");
bool replicateAllPossibleTables = Console.ReadLine().Equals("Y", StringComparison.OrdinalIgnoreCase);

Console.WriteLine("Pass null for temporal? (Y/N)");
bool passNullForValidFromValidTo = Console.ReadLine().Equals("Y", StringComparison.OrdinalIgnoreCase);

string sourceConnectionString, targetConnectionString;
var dummy = "";

if (replicateToLocalDb)
{
    string sourceDatabaseName = "";
    string targetDatabaseName = "";
    string serverName = "";
    sourceConnectionString = $"Persist Security Info=True;Server={serverName};Authentication=Active Directory Interactive;Database={sourceDatabaseName};Encrypt=True;TrustServerCertificate=True;MultipleActiveResultSets=True";
    targetConnectionString = $"Server=tcp:localhost,1433;Initial Catalog={targetDatabaseName};User ID={dummy};Password={dummy};Encrypt=True;TrustServerCertificate=True;";
}
else
{
    //(Rare: there will be instance where data needs to be replicated from SIT to DEV)
    string sourceDatabaseName = "";
    string targetDatabaseName = "";
    string serverName = "";
    sourceConnectionString = $"Persist Security Info=True;Server={serverName};Authentication=Active Directory Interactive;Database={sourceDatabaseName};Encrypt=True;TrustServerCertificate=True;MultipleActiveResultSets=True";
    targetConnectionString = $"Persist Security Info=True;Server={serverName};Authentication=Active Directory Interactive;Database={targetDatabaseName};Encrypt=True;TrustServerCertificate=True;MultipleActiveResultSets=True";
}

Dictionary<string, string> sourceToDestinationTableMappingDict = new()
{
    // {"Tax", "ReferenceTax"}
};

List<ExceptionInfo> failureInfo = new();

try
{
    if (replicateAllTablesIncludingRtTables)
    {
        ReplicateAcross(passNullForValidFromValidTo, sourceConnectionString, targetConnectionString, failureInfo);
        ReplicateSpecified(passNullForValidFromValidTo, sourceConnectionString, targetConnectionString, sourceToDestinationTableMappingDict, failureInfo);
    }
    else
    {
        if (replicateAllPossibleTables)
        {
            ReplicateAcross(passNullForValidFromValidTo, sourceConnectionString, targetConnectionString, failureInfo);
        }
        else
        {
            ReplicateSpecified(passNullForValidFromValidTo, sourceConnectionString, targetConnectionString, sourceToDestinationTableMappingDict, failureInfo);
        }
    }

    if (failureInfo.Count > 0)
        Console.WriteLine($"Failed for the following: {string.Join(Environment.NewLine, failureInfo)}");
    else
        Console.WriteLine("No failure!");

    Console.ReadLine();
}
catch (Exception ex)
{
    Console.WriteLine($"An error occurred: {ex.Message}");
}

static DataTable GetTableData(string connectionString, string tableName, bool schemaOnly = false)
{
    string selectQuery = schemaOnly ? $"SELECT TOP 0 * FROM {tableName}" : $"SELECT * FROM {tableName}";
    DataTable data = new();
    using SqlConnection connection = new(connectionString);
    using SqlCommand command = new(selectQuery, connection);
    using SqlDataAdapter adapter = new(command);
    connection.Open();
    adapter.Fill(data);
    return data;
}

static void Replicate(string sourceConnectionString, string destinationConnectionString, string sourceTableName, string destinationTableName, List<ExceptionInfo> failure, bool passNullForValidFromValidTo)
{
    try
    {
        if (HasData(destinationConnectionString, destinationTableName))
        {
            Console.WriteLine($"Destination {destinationConnectionString} {destinationTableName} has data, skipping..");
            return;
        }

        DataTable sourceData = GetTableData(sourceConnectionString, sourceTableName);
        Dictionary<string, ColumnInfo> destinationColumnsDict = GetColumnsInfo(destinationConnectionString, destinationTableName);
        bool isTemporalTable = IsTemporalTable(destinationConnectionString, destinationTableName);

        if (isTemporalTable)
            HandleTemporalTableColumns(sourceData, destinationColumnsDict);

        var destinationColumns = destinationColumnsDict.Keys.ToList();
        if (destinationColumns.Count == 0)
        {
            DataTable destinationStructure = GetTableData(destinationConnectionString, destinationTableName, true);
            destinationColumnsDict = destinationStructure.Columns.Cast<DataColumn>().ToDictionary(x => x.ColumnName, x => new ColumnInfo
            {
                DataType = x.DataType,
                IsNullable = x.AllowDBNull
            });
            destinationColumns = destinationStructure.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        }

        foreach (string columnName in destinationColumns)
        {
            var destinationColumnInfo = destinationColumnsDict[columnName];
            if (!sourceData.Columns.Contains(columnName) || sourceData.Columns[columnName].DataType.Name != destinationColumnInfo.DataType.Name)
            {
                if (sourceData.Columns.Contains(columnName)) sourceData.Columns.Remove(columnName);
                sourceData.Columns.Add(columnName, destinationColumnInfo.DataType);
                foreach (DataRow row in sourceData.Rows)
                    row[columnName] = GetDefaultValue(destinationColumnInfo.DataType);

                Console.WriteLine($"Added column '{columnName}' to source data with default value for '{destinationColumnInfo.DataType}'.");
            }
        }

        using SqlConnection destinationConnection = new(destinationConnectionString);
        using SqlBulkCopy bulkCopy = new(destinationConnection) { DestinationTableName = destinationTableName };
        destinationConnection.Open();

        foreach (string columnName in destinationColumns)
        {
            bool isValidFromOrValidTo = columnName.Equals("DateTimeValidFrom") || columnName.Equals("DateTimeValidTo");
            if (!isValidFromOrValidTo || (isValidFromOrValidTo && !passNullForValidFromValidTo))
                bulkCopy.ColumnMappings.Add(columnName, columnName);
        }

        DataTable dataToWrite = sourceData.Clone();
        foreach (DataRow sourceRow in sourceData.Rows)
        {
            DataRow newRow = dataToWrite.NewRow();
            foreach (DataColumn column in sourceData.Columns)
            {
                destinationColumnsDict.TryGetValue(column.ColumnName, out var columnInfo);
                newRow[column.ColumnName] = sourceRow.IsNull(column) && columnInfo is not null && !columnInfo.IsNullable
                    ? GetDefaultValue(column.DataType)
                    : sourceRow[column];
            }
            dataToWrite.Rows.Add(newRow);
        }

        bulkCopy.WriteToServer(dataToWrite);
        Console.WriteLine($"Data replication completed successfully for {sourceTableName}.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"An error occurred: {ex.Message}");
        failure.Add(new ExceptionInfo { TableName = destinationTableName, ExceptionMessage = ex.Message });
    }
}

static void HandleTemporalTableHistory(DataTable dataTable, string connectionString, string tableName)
{
    string historyTableName = $"{tableName}History";
    Dictionary<string, ColumnInfo> historyColumnsDict = GetColumnsInfo(connectionString, historyTableName);
    DataTable historyData = dataTable.Clone();

    historyData.Columns.Add(new DataColumn("DateTimeValidFrom", typeof(DateTime)) { DefaultValue = DateTime.Now });
    historyData.Columns.Add(new DataColumn("DateTimeValidTo", typeof(DateTime)) { DefaultValue = DateTime.MaxValue });

    foreach (DataRow row in dataTable.Rows)
    {
        DataRow historyRow = historyData.NewRow();
        foreach (DataColumn column in dataTable.Columns)
            historyRow[column.ColumnName] = row[column];
        historyData.Rows.Add(historyRow);
    }

    using SqlConnection connection = new(connectionString);
    using SqlBulkCopy bulkCopy = new(connection) { DestinationTableName = historyTableName };
    connection.Open();

    foreach (string columnName in historyColumnsDict.Keys)
        bulkCopy.ColumnMappings.Add(columnName, columnName);

    bulkCopy.WriteToServer(historyData);
}

static bool IsTemporalTable(string connectionString, string tableName)
{
    using SqlConnection connection = new(connectionString);
    connection.Open();
    SqlCommand command = new($"SELECT temporal_type FROM sys.tables WHERE object_id = OBJECT_ID('{tableName}', 'u')", connection);
    object result = command.ExecuteScalar();
    return result != null && (byte)result == 2;
}

static void HandleTemporalTableColumns(DataTable dataTable, Dictionary<string, ColumnInfo> columnsDict)
{
    string startColumn = "DateTimeValidFrom", endColumn = "DateTimeValidTo";
    if (dataTable.Columns.Contains(startColumn) && dataTable.Columns.Contains(endColumn))
    {
        foreach (DataRow row in dataTable.Rows)
        {
            row[startColumn] = DateTime.Now;
            row[endColumn] = DateTime.MaxValue;
        }
    }
}

static List<string> ListDatabaseTables(string connectionString)
{
    List<string> tables = new();
    try
    {
        using SqlConnection connection = new(connectionString);
        connection.Open();
        Console.WriteLine($"Connected to the database successfully {connectionString}.");

        string query = @"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES";

        using SqlCommand command = new(query, connection);
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
            tables.Add(reader["TABLE_NAME"].ToString());
    }
    catch (Exception ex)
    {
        Console.WriteLine($"An error occurred: {ex.Message}");
    }
    return tables;
}

static bool HasData(string connectionString, string tableName)
{
    using SqlConnection connection = new(connectionString);
    using SqlCommand command = new($"SELECT TOP 1 1 FROM {tableName}", connection);
    connection.Open();
    return command.ExecuteScalar() != null;
}

static Dictionary<string, ColumnInfo> GetColumnsInfo(string connectionString, string tableName)
{
    Dictionary<string, ColumnInfo> columns = new();
    string query = @"SELECT c.name AS ColumnName, t.name AS DataType, c.max_length AS MaxLength, c.precision AS Precision,
                     c.scale AS Scale, c.is_nullable AS IsNullable, c.is_computed AS IsComputed, c.generated_always_type AS IsGeneratedAlwaysType
                     FROM sys.columns c INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                     INNER JOIN sys.tables tab ON c.object_id = tab.object_id WHERE tab.name = @TableName ORDER BY c.column_id";

    using SqlConnection connection = new(connectionString);
    using SqlCommand command = new(query, connection);
    command.Parameters.AddWithValue("@TableName", tableName);
    connection.Open();

    using SqlDataReader reader = command.ExecuteReader();
    while (reader.Read())
    {
        columns.Add(reader["ColumnName"].ToString(), new ColumnInfo
        {
            DataType = GetClrType(reader["DataType"].ToString(), Convert.ToInt32(reader["MaxLength"]), Convert.ToInt32(reader["Precision"]), Convert.ToInt32(reader["Scale"])),
            IsNullable = Convert.ToBoolean(reader["IsNullable"])
        });
    }
    return columns;
}

static Type GetClrType(string sqlType, int maxLength, int precision, int scale) => sqlType.ToLower() switch
{
    "bigint" => typeof(long),
    "binary" or "image" or "varbinary" => typeof(byte[]),
    "bit" => typeof(bool),
    "char" or "nchar" or "ntext" or "nvarchar" or "text" or "varchar" => typeof(string),
    "date" or "datetime" or "datetime2" or "smalldatetime" => typeof(DateTime),
    "decimal" or "money" or "numeric" or "smallmoney" => typeof(decimal),
    "float" => typeof(double),
    "int" => typeof(int),
    "real" => typeof(float),
    "smallint" => typeof(short),
    "time" => typeof(TimeSpan),
    "tinyint" => typeof(byte),
    "uniqueidentifier" => typeof(Guid),
    "datetimeoffset" => typeof(DateTimeOffset),
    "timestamp" => typeof(int),
    _ => typeof(object)
};

static object GetDefaultValue(Type type) => type == typeof(string) ? string.Empty :
    type == typeof(int) || type == typeof(float) || type == typeof(short) ? 0 :
    type.IsValueType ? Activator.CreateInstance(type) : null;

static void ReplicateAcross(bool passNullForValidFromValidTo, string sourceConnectionString, string targetConnectionString, List<ExceptionInfo> failureInfo)
{
    var sourceTables = ListDatabaseTables(sourceConnectionString);
    var destinationTables = ListDatabaseTables(targetConnectionString);
    List<string> identicalTables = sourceTables.Intersect(destinationTables).ToList();

    Console.WriteLine($"Total number of identical tables: {identicalTables.Count}");
    Console.WriteLine($"The tables are: {string.Join(", ", identicalTables)}");

    foreach (string tableName in identicalTables)
    {
        Console.WriteLine($"Processing {tableName}");
        Replicate(sourceConnectionString, targetConnectionString, tableName, tableName, failureInfo, passNullForValidFromValidTo);
    }
}

static void ReplicateSpecified(bool passNullForValidFromValidTo, string sourceConnectionString, string targetConnectionString, Dictionary<string, string> sourceToDestinationDict, List<ExceptionInfo> failureInfo)
{
    foreach (var kvp in sourceToDestinationDict)
    {
        Console.WriteLine($"Processing {kvp.Key}");
        Replicate(sourceConnectionString, targetConnectionString, kvp.Key, kvp.Value, failureInfo, passNullForValidFromValidTo);
    }
}

class ExceptionInfo
{
    public string TableName { get; set; }
    public string ExceptionMessage { get; set; }
    public override string ToString() => $"TableName: {TableName}, ExceptionMessage: {ExceptionMessage}";
}

class ColumnInfo
{
    public Type DataType { get; set; }
    public bool IsNullable { get; set; }
}
