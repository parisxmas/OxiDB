using Test
using OxiDb

# Spin up a server and point OXIDB_TEST_PORT at it before running:
#   OXIDB_TEST_PORT=4444 julia --project=. test/runtests.jl
const PORT = parse(Int, get(ENV, "OXIDB_TEST_PORT", "4444"))
const HOST = get(ENV, "OXIDB_TEST_HOST", "127.0.0.1")
const COL  = "_jl_test"

@testset "OxiDb minimal client" begin
    OxiDb.connect(HOST, PORT) do db
        @test ping(db) == "pong"

        # Wipe any prior run via document-level delete (drop_collection is broken
        # on some server builds — Issue: "Not a directory" on fresh data dirs).
        delete(db, COL, Dict{String,Any}())

        insert(db, COL, Dict("name" => "Alice", "age" => 30, "active" => true))
        insert(db, COL, Dict("name" => "Bob",   "age" => 17, "active" => true))
        insert(db, COL, Dict("name" => "Carol", "age" => 42, "active" => false))

        @test count_docs(db, COL) == 3
        @test count_docs(db, COL, Dict("active" => true)) == 2

        adults = find(db, COL; query = Dict("age" => Dict("\$gte" => 18)),
                                sort  = Dict("age" => -1), limit = 10)
        @test length(adults) == 2
        @test adults[1]["name"] == "Carol"

        alice = find_one(db, COL, Dict("name" => "Alice"))
        @test alice !== nothing
        @test alice["age"] == 30

        update(db, COL, Dict("name" => "Alice"),
               Dict("\$inc" => Dict("age" => 1)))
        @test find_one(db, COL, Dict("name" => "Alice"))["age"] == 31

        delete(db, COL, Dict("active" => false))
        @test count_docs(db, COL) == 2

        # Generic exec — anything not in the convenience list.
        procs = exec(db, "list_procedures")
        @test procs isa AbstractVector

        # Cleanup
        delete(db, COL, Dict{String,Any}())
        @test count_docs(db, COL) == 0
    end
end
