#!/usr/bin/env julia
# 01_hello.jl — embedded OxiDB: the engine runs *in your process*. No server,
# no socket, no separate process to manage. Smallest possible program.

using OxiDbEmbedded

db = open_db(mktempdir())          # a fresh on-disk database in a temp dir

insert(db, "users", Dict("name" => "Alice", "age" => 30, "email" => "alice@example.com"))

alice = find_one(db, "users", Dict("name" => "Alice"))
println("Loaded: ", alice)

close(db)
