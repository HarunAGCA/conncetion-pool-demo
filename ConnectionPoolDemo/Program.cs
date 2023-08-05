
using Newtonsoft.Json;
using Npgsql;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text.Json.Serialization;

const string SQLSERVER = nameof(SQLSERVER);
const string POSTGRES = nameof(POSTGRES);


string selectedDb;


if (args.Length == 0 || string.IsNullOrWhiteSpace(args[0]))
{
    Console.WriteLine($"Select DB: ({SQLSERVER} or {POSTGRES})");
    selectedDb = Console.ReadLine().Trim().ToUpperInvariant() ?? "";
}
else
{
    selectedDb = args[0].Trim().ToUpperInvariant();
}

if (!(selectedDb == SQLSERVER || selectedDb == POSTGRES))
{
    Console.WriteLine("Unsupported Db");
}

switch (selectedDb)
{

    case SQLSERVER:
        await new SqlServer().PerformSqlServerOperations();
        break;
    case POSTGRES:
        await new Postgres().PerformPostgresOperations();
        break;
    default:
        Console.WriteLine("Unsupported DB");
        return;

}

public class SqlServer
{
    const string SQLSERVER_CONNECTION_STRING = "Server=127.0.0.1;Database=PoolingDemo;Trusted_Connection=True;Max pool size=10; Min pool size=5";
    public async Task PerformSqlServerOperations()
    {
        List<SqlConnection> connections = new();

        for (int i = 0; i < 10; i++)
        {
            Console.ReadKey();
            var connection = new SqlConnection(SQLSERVER_CONNECTION_STRING);
            await connection.OpenAsync();
            connections.Add(connection);

            SelectSqlServer(connection);
            await InsertSqlServer(connection);
            await UpdateSqlServer(connection);
            await DeleteSqlServer(connection);

            Console.WriteLine($"Connection has been opened with conection id : ${connections[i].ClientConnectionId}");
        }

        foreach (var connction in connections)
        {
            await connction.CloseAsync();
        }
    }

    private void SelectSqlServer(SqlConnection connection)
    {
        using SqlCommand command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM UserTable";

        SqlDataAdapter dataAdapter = new SqlDataAdapter(command);

        DataTable dt = new();
        dataAdapter.Fill(dt);

        Console.WriteLine(JsonConvert.SerializeObject(dt));
    }

    private async Task InsertSqlServer(SqlConnection connection)
    {
        using SqlCommand command = new() { Connection = connection, CommandType = System.Data.CommandType.Text, CommandText = "INSERT INTO UserTable ( FirstName, LastName, Email, Age)VALUES ( 'John', 'Doe', 'john.doe@example.com', 30);" };

        int affectedCount = await command.ExecuteNonQueryAsync();

        Console.WriteLine($"affectedCount after Insert : {affectedCount}");
    }

    private async Task UpdateSqlServer(SqlConnection connection)
    {
        using SqlCommand command = new() { Connection = connection, CommandType = System.Data.CommandType.Text, CommandText = "UPDATE UserTable\r\nSET Age = 31\r\nWHERE UserId = 1;\r\n" };

        int affectedCount = await command.ExecuteNonQueryAsync();

        Console.WriteLine($"affectedCount after Update : {affectedCount}");
    }

    private async Task DeleteSqlServer(SqlConnection connection)
    {
        using SqlCommand command = new() { Connection = connection, CommandType = System.Data.CommandType.Text, CommandText = "DELETE FROM UserTable\r\nWHERE UserId = 1;\r\n" };

        int affectedCount = await command.ExecuteNonQueryAsync();

        Console.WriteLine($"affectedCount after delete : {affectedCount}");
    }
}

public class Postgres
{
    const string POSTGRES_CONNECTION_STRING = "Server=127.0.0.1;Port=5432;Database=PoolingDemo;username=postgres;password=postgres_password;Pooling=true;MaxPoolSize=8;MinPoolSize=5;";
    public async Task PerformPostgresOperations()
    {
        List<NpgsqlConnection> connections = new();

        for (int i = 0; i < 10; i++)
        {
            Console.ReadKey();

            NpgsqlConnection connection = new NpgsqlConnection(POSTGRES_CONNECTION_STRING);
            await connection.OpenAsync();
            connections.Add(connection);

            SelectPostgres(connection);
            await InsertPostgres(connection);
            await UpdatePostgres(connection);
            await DeletePostgres(connection);


            Console.WriteLine($"Connection has been opened with process id : {connections[i].ProcessID}");
        }

        foreach (var connction in connections)
        {
            await connction.CloseAsync();
        }
    }

    private void SelectPostgres(NpgsqlConnection connection)
    {
        using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM UserTable";

        NpgsqlDataAdapter dataAdapter = new NpgsqlDataAdapter(command);

        DataTable dt = new();
        dataAdapter.Fill(dt);

        Console.WriteLine(JsonConvert.SerializeObject(dt));
    }

    private async Task InsertPostgres(NpgsqlConnection connection)
    {
        using NpgsqlCommand command = connection.CreateCommand();
        command.CommandText = "INSERT INTO UserTable ( FirstName, LastName, Email, Age)VALUES ( 'John', 'Doe', 'john.doe@example.com', 30);";
        int affectedCount = await command.ExecuteNonQueryAsync();

        Console.WriteLine($"affectedCount after Insert: {affectedCount}");
    }

    private async Task UpdatePostgres(NpgsqlConnection connection)
    {
        using NpgsqlCommand command = new() { Connection = connection, CommandType = System.Data.CommandType.Text, CommandText = "UPDATE UserTable\r\nSET Age = 31\r\nWHERE UserId = 1;\r\n" };

        int affectedCount = await command.ExecuteNonQueryAsync();

        Console.WriteLine($"affectedCount after update: {affectedCount}");
    }

    private async Task DeletePostgres(NpgsqlConnection connection)
    {
        using NpgsqlCommand command = new() { Connection = connection, CommandType = System.Data.CommandType.Text, CommandText = "DELETE FROM UserTable\r\nWHERE UserId = 1;\r\n" };

        int affectedCount = await command.ExecuteNonQueryAsync();

        Console.WriteLine($"affectedCount after delete: {affectedCount}");
    }
}






