using Npgsql;
using Npgsql.Age;
using Npgsql.Age.Types;

namespace apache_age_console;

public static class AgeHelpers
{
    public const string RESULT_SHAPE = "r agtype";

    private static ISet<IAsyncDisposable> refs = new HashSet<IAsyncDisposable>(ReferenceEqualityComparer.Instance);

    public static async Task Dispose()
    {
        foreach (var @ref in refs)
        {
            await @ref.DisposeAsync();
        }
    }
    
    public static NpgsqlDataSourceBuilder AgeDataSourceBuilder(
        string connectionString,
        Action<NpgsqlConnectionStringBuilder>? connectionStringPostGenerator = null,
        Action<NpgsqlDataSourceBuilder>? datasourcePostGenerator = null)
    {
        var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "ag_catalog, \"$user\", public"
        };
        connectionStringPostGenerator?.Invoke(connectionStringBuilder);

        var datasourceBuilder = new NpgsqlDataSourceBuilder(connectionStringBuilder.ConnectionString)
            .UseAge();
        datasourcePostGenerator?.Invoke(datasourceBuilder);

        return datasourceBuilder;
    }

    public static async Task CreateGraphIfNotExists(NpgsqlDataSource source, string graph)
    {
        bool exists;

        await using (var connection = await source.OpenConnectionAsync())
        {
            await using var existsCmd = connection.GraphExistsCommand(graph);
            exists = (bool)(await existsCmd.ExecuteScalarAsync())!;
        }

        if (exists)
            return;

        await using (var connection = await source.OpenConnectionAsync())
        {
            await using var createCmd = connection.CreateGraphCommand(graph);
            await createCmd.ExecuteNonQueryAsync();
        }
    }

    public static async Task<NpgsqlDataReader> CypherReader(
        NpgsqlDataSource datasource, 
        string graph, 
        string cypher,
        string resultShape = RESULT_SHAPE)
    {
        var cmd = await CypherCommand(datasource, graph, cypher, resultShape);
        return await cmd.ExecuteReaderAsync();
    }
    
    public static async Task<NpgsqlDataReader> CypherReader(
        NpgsqlConnection connection, 
        string graph, 
        string cypher,
        string resultShape = RESULT_SHAPE)
    {
        var cmd = await CypherCommand(connection, graph, cypher, resultShape);
        return await cmd.ExecuteReaderAsync();
    }
    
    public static async Task<NpgsqlDataReader> CypherReader(
        NpgsqlConnection connection, 
        string graph, 
        string cypher,
        string jsonData,
        string resultShape = RESULT_SHAPE)
    {
        var cmd = await CypherCommand(connection, graph, cypher, jsonData, resultShape);
        return await cmd.ExecuteReaderAsync();
    }
    
    public static async Task<NpgsqlDataReader> CypherReader(
        NpgsqlDataSource datasource, 
        string graph, 
        string cypher,
        string jsonData,
        string resultShape = RESULT_SHAPE)
    {
        var cmd = await CypherCommand(datasource, graph, cypher, jsonData, resultShape);
        return await cmd.ExecuteReaderAsync();
    }

    public static async Task<int> CypherNonQueryAsync(
        NpgsqlDataSource datasource, 
        string graph, 
        string cypher)
    {
        var cmd = await CypherCommand(datasource, graph, cypher, RESULT_SHAPE);
        return await cmd.ExecuteNonQueryAsync();
    }
    
    public static async Task<int> CypherNonQueryAsync(
        NpgsqlDataSource datasource, 
        string graph, 
        string cypher,
        string jsonData)
    {
        var cmd = await CypherCommand(datasource, graph, cypher, jsonData, RESULT_SHAPE);
        return await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<NpgsqlCommand> CypherCommand(
        NpgsqlDataSource source, 
        string graph, 
        string cypher,
        string resultShape = RESULT_SHAPE)
    {
        var connection = await source.OpenConnectionAsync();
        refs.Add(connection);
        
        return CreateCypherCommand(connection, graph, cypher, resultShape);
    }
    
    public static async Task<NpgsqlCommand> CypherCommand(
        NpgsqlConnection source, 
        string graph, 
        string cypher,
        string resultShape = RESULT_SHAPE)
    {
        return CreateCypherCommand(source, graph, cypher, resultShape);
    }

    public static NpgsqlCommand CreateCypherCommand(
        NpgsqlConnection connection,
        string graph,
        string cypher,
        string resultShape)
    {
        var command = new NpgsqlCommand(@$"SELECT * FROM cypher('{graph}', $$ {cypher} $$) AS ({resultShape});", connection);
        refs.Add(command);
        
        return command;
    }

    public static async Task<NpgsqlCommand> CypherCommand(
        NpgsqlDataSource source, 
        string graph, 
        string cypher,
        string jsonData, 
        string resultShape = RESULT_SHAPE)
    {
        var connection = await source.OpenConnectionAsync();
        refs.Add(connection);
        
        return CreateCypherCommandWithParameters(connection, graph, cypher, jsonData, resultShape);
    }
    
    public static async Task<NpgsqlCommand> CypherCommand(
        NpgsqlConnection source, 
        string graph, 
        string cypher,
        string jsonData, 
        string resultShape = RESULT_SHAPE)
    {
        return CreateCypherCommandWithParameters(source, graph, cypher, jsonData, resultShape);
    }

    public static NpgsqlCommand ConnectionlessCypherCommand(
        string graph, 
        string cypher,
        string resultShape = RESULT_SHAPE)
    {
        return CreateCypherCommandWithParameters(null!, graph, cypher, null!, resultShape);
    }

    public static NpgsqlCommand ConnectCommand(NpgsqlCommand command, NpgsqlDataSource datasource)
    {
        var connection = datasource.OpenConnection();
        refs.Add(connection);
        
        command.Connection = connection;
        return command;
    }

    public static NpgsqlCommand AddParameter(NpgsqlCommand command, string jsonData, string? parameterName = null)
    {
        command.Parameters.Add(new NpgsqlParameter
        {
            Value = new Agtype(jsonData), 
            DataTypeName = "agtype",
            ParameterName = parameterName ?? string.Empty,
        });
        
        return command;
    }
    
    public static NpgsqlCommand CreateCypherCommandWithParameters(
        NpgsqlConnection connection,
        string graph,
        string cypher,
        string jsonData,
        string resultShape = RESULT_SHAPE)
        => new NpgsqlCommand(@$"SELECT * FROM cypher('{graph}', $$ {cypher} $$, $1) AS ({resultShape});", connection)
        {
            Parameters =
            {
                new NpgsqlParameter
                {
                    Value = new Agtype(jsonData),
                    DataTypeName = "agtype"
                },
            }
        };
}