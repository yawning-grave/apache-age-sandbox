using System.ComponentModel.DataAnnotations;
using System.ComponentModel.Design;
using System.Data;
using System.Text.Json;
using Npgsql;
using Npgsql.Age;
using Npgsql.Age.Types;
using static apache_age_console.AgeHelpers;

const string connectionString = "Host=localhost;Port=5432;Username=user;Password=password;Database=postgres";
const string graph = "test_graph2";

var datasource = AgeDataSourceBuilder(connectionString).Build();

AppContext.SetSwitch("Npgsql.EnableSqlRewriting", true);

// await CreateAndReadNodes();
await CreateParameterizedNodes();

async Task CreateAndReadNodes()
{
    await CreateGraphIfNotExists(datasource, graph);
    await CypherNonQueryAsync(datasource, graph, "CREATE (:Person {age: 23}), (:Person {age: 78})");

    await using var reader = await CypherReader(datasource, graph, "MATCH (n:Person) RETURN n", "person agtype");
    while (await reader.ReadAsync())
    {
        var agtypeResult = (Agtype)reader["person"]; // reader.GetFieldValue<Agtype>(0);
        Vertex person = agtypeResult.GetVertex();
        Console.WriteLine(person);
    }
}

async Task CreateParameterizedNodes()
{
    await CreateGraphIfNotExists(datasource, graph);
    await CypherNonQueryAsync(datasource, graph, "MATCH (n) DETACH DELETE n");

    await CypherNonQueryAsync(datasource, graph,
        "CREATE (p:Person) SET p.name = $name, p.age = $age",
        @"{""name"": ""Alice"", ""age"": 24}");

    await CypherNonQueryAsync(datasource, graph,
        "CREATE (p:Person { age: $age, name: $name })",
        JsonSerializer.Serialize(new { name = "Bob", age = 23 }));

    await CypherNonQueryAsync(datasource, graph,
        "CREATE (p:Person { age: $age, name: $name })",
        JsonSerializer.Serialize(new Dictionary<string, object> { ["name"] = "Candace", ["age"] = 25 }));

    await using var connection = datasource.CreateConnection();
    await connection.OpenAsync();

    var data = JsonSerializer.Serialize(
        new
        {
            rows = new object[]
            {
                new { abFriends = new { since = "2023" } },
                new { bcColleagues = new { since = "2020", job = "software-dev" } },
                new { bcHasCrushOn = new { since = "hello?" } }
            }
        });

    // Direct cypher query parametrization
    var cmd = new NpgsqlCommand(@$"
        SELECT * FROM cypher('{graph}', $$
            UNWIND $rows AS row
            CREATE (p:Person) SET p = row
            RETURN p
        $$, @datarows) AS (r agtype);", connection);
    cmd.Parameters.Add(new NpgsqlParameter("datarows", new Agtype(data)) { DataTypeName = "agtype" });
    var reader = await cmd.ExecuteNonQueryAsync();


    var pstmt = new NpgsqlCommand($@"PREPARE ag_create_people(agtype) AS
            SELECT * FROM cypher('{graph}', $$ 
                UNWIND $rows AS row
                CREATE (p:Person) SET p = row
                RETURN p
        $$, $1) AS (r agtype);
    ", connection);

    await pstmt.ExecuteNonQueryAsync();


    // var exec = new NpgsqlCommand("EXECUTE ag_create_people(@data)", connection);
    // var param = new NpgsqlParameter("data", new Agtype(data));
    // // Specify the DataTypeName to inform Npgsql to serialize as agtype
    // param.DataTypeName = "agtype";
    // exec.Parameters.Add(param);

    // var exec = new NpgsqlCommand($"EXECUTE ag_create_people('{data}')", connection);
    //
    // await exec.ExecuteNonQueryAsync();

    await using var pstmt2 = new NpgsqlCommand(
        @$"SELECT * FROM cypher('{graph}', $$ 
                MATCH (a:Person {{ name: 'Alice' }}), 
                    (b:Person {{ name: 'Bob' }}), 
                    (c:Person {{ name: 'Candace'}}) 
                UNWIND $rows AS row
                WITH row.abFriends AS map_abFriends,
                    row.bcColleagues AS map_bcColleagues,
                    row.bcHasCrushOn AS map_bcHasCrushOn
                CREATE (a)-[abFriends:FRIENDS]->(b),
                    (b)-[baFriends:FRIENDS]->(a), 
                    (b)-[bcColleagues:COLLEAGUES]->(c),
                    (c)-[cbColleagues:COLLEAGUES]->(b),
                    (b)-[bcHasCrushOn:HAS_CRUSH_ON]->(c)
                SET abFriends = map_abFriends,
                    baFriends = map_abFriends,
                    bcColleagues = map_bcColleagues,
                    cbColleagues = map_bcColleagues,                    
                    bcHasCrushOn = map_bcHasCrushOn
                RETURN a, b, c, abFriends, bcColleagues, bcHasCrushOn
        $$, @datarowsii) AS (a agtype, b agtype, c agtype, abFriends agtype, bcColleagues agtype, bcHasCrushOn agtype);",
        connection);

    var data2 = JsonSerializer.Serialize(
        new
        {
            rows = new object[]
            {
                new
                {
                    abFriends = new { since = "2023" },
                    bcColleagues = new { since = "2020" },
                    bcHasCrushOn = new { since = "forever" }
                }
            }
        });

    pstmt2.Parameters.Add(new NpgsqlParameter("datarowsii", new Agtype(data2)) { DataTypeName = "agtype" });
    Vertex a = default, b = default, c = default;
    Edge abFriends = default, bcColleagues = default, bcHasCrushOn = default;

    var execReader = await pstmt2.ExecuteReaderAsync();

    /*while (await execReader.ReadAsync())
    {
        a = ((Agtype)execReader["a"]).GetVertex();
        b = ((Agtype)execReader["b"]).GetVertex();
        c = ((Agtype)execReader["c"]).GetVertex();
        abFriends = ((Agtype)execReader["abFriends"]).GetEdge();
        bcColleagues = ((Agtype)execReader["bcColleagues"]).GetEdge();
        bcHasCrushOn = ((Agtype)execReader["bcHasCrushOn"]).GetEdge();

        break;
    }

    Console.WriteLine(a);
    Console.WriteLine(b);
    Console.WriteLine(c);
    Console.WriteLine(abFriends);
    Console.WriteLine(bcColleagues);
    Console.WriteLine(bcHasCrushOn);*/
}