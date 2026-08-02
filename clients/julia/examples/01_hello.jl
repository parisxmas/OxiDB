#!/usr/bin/env julia
# 01_hello.jl — connect, insert, read back, close.
# Smallest possible Julia program against OxiDB.

using OxiDb

OxiDb.connect("127.0.0.1", 4444) do db
    @show ping(db)

    insert(db, "users", Dict(
        "name"  => "Alice",
        "age"   => 30,
        "email" => "alice@example.com",
    ))

    alice = find_one(db, "users", Dict("name" => "Alice"))
    println("Loaded: ", alice)
end
