#!/usr/bin/env julia
#
# Quick example connecting to the remote OxiDB server.
#
# Run: cd examples/julia && julia --project=. remote_example.jl
#

using OxiDb

client = connect_oxidb("127.0.0.1", 4444)

println("Ping: ", ping(client))

insert(client, "users", Dict("name" => "Alice", "age" => 30))
insert(client, "users", Dict("name" => "Bob", "age" => 25))

docs = find(client, "users", Dict("name" => "Alice"))
println("Find Alice: ", docs)

n = count_docs(client, "users")
println("Total users: ", n)

delete(client, "users", Dict())
drop_collection(client, "users")
println("Cleanup done.")

close(client)
println("Done!")
