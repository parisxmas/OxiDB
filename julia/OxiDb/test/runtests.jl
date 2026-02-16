using Test
using OxiDb

const HOST = get(ENV, "OXIDB_HOST", "127.0.0.1")
const PORT = parse(Int, get(ENV, "OXIDB_PORT", "4444"))

@testset "OxiDb Julia Client" begin
    client = connect_oxidb(HOST, PORT)

    @testset "ping" begin
        @test ping(client) == "pong"
    end

    @testset "collections" begin
        create_collection(client, "jl_test")
        cols = list_collections(client)
        @test "jl_test" in cols
    end

    @testset "insert & find" begin
        result = insert(client, "jl_test", Dict("name" => "Alice", "age" => 30))
        @test haskey(result, "id")

        docs = find(client, "jl_test", Dict("name" => "Alice"))
        @test length(docs) >= 1
        @test docs[1]["name"] == "Alice"
        @test docs[1]["age"] == 30
    end

    @testset "insert_many" begin
        result = insert_many(client, "jl_test", [
            Dict("name" => "Bob", "age" => 25),
            Dict("name" => "Charlie", "age" => 35)
        ])
        @test length(result) == 2
    end

    @testset "find with options" begin
        docs = find(client, "jl_test", Dict(); sort=Dict("age" => 1))
        @test length(docs) >= 3

        docs = find(client, "jl_test", Dict(); limit=1)
        @test length(docs) == 1
    end

    @testset "find_one" begin
        doc = find_one(client, "jl_test", Dict("name" => "Bob"))
        @test doc["name"] == "Bob"
    end

    @testset "count" begin
        n = count_docs(client, "jl_test")
        @test n >= 3
    end

    @testset "update" begin
        result = update(client, "jl_test",
                        Dict("name" => "Alice"),
                        Dict("\$set" => Dict("age" => 31)))
        @test result["modified"] == 1

        doc = find_one(client, "jl_test", Dict("name" => "Alice"))
        @test doc["age"] == 31
    end

    @testset "delete" begin
        result = delete(client, "jl_test", Dict("name" => "Charlie"))
        @test result["deleted"] == 1
    end

    @testset "indexes" begin
        create_index(client, "jl_test", "name")
        create_unique_index(client, "jl_test", "age")
        create_composite_index(client, "jl_test", ["name", "age"])
    end

    @testset "aggregation" begin
        result = aggregate(client, "jl_test", [
            Dict("\$group" => Dict("_id" => nothing, "avg_age" => Dict("\$avg" => "\$age")))
        ])
        @test length(result) >= 1
    end

    @testset "transactions" begin
        transaction(client) do
            insert(client, "jl_tx", Dict("action" => "debit", "amount" => 100))
            insert(client, "jl_tx", Dict("action" => "credit", "amount" => 100))
        end
        docs = find(client, "jl_tx")
        @test length(docs) == 2
    end

    @testset "blob storage" begin
        create_bucket(client, "jl-bucket")

        buckets = list_buckets(client)
        @test "jl-bucket" in buckets

        put_object(client, "jl-bucket", "test.txt",
                   Vector{UInt8}("Hello from Julia!"))

        data, meta = get_object(client, "jl-bucket", "test.txt")
        @test String(data) == "Hello from Julia!"

        head = head_object(client, "jl-bucket", "test.txt")
        @test haskey(head, "size")

        objs = list_objects(client, "jl-bucket")
        @test length(objs) >= 1

        delete_object(client, "jl-bucket", "test.txt")
    end

    @testset "search" begin
        results = search(client, "hello")
        @test isa(results, AbstractVector)
    end

    @testset "compact" begin
        stats = compact(client, "jl_test")
        @test haskey(stats, "docs_kept")
    end

    @testset "cleanup" begin
        drop_collection(client, "jl_test")
        drop_collection(client, "jl_tx")
        delete_bucket(client, "jl-bucket")
    end

    close(client)
end
