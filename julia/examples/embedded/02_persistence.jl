#!/usr/bin/env julia
# 02_persistence.jl — the embedded database is a real on-disk store, like
# SQLite. Close it, reopen the same path, and the data is still there.

using OxiDbEmbedded

path = mktempdir()

# Session 1 — write, then close.
let db = open_db(path)
    insert_many(db, "notes", [
        Dict("title" => "buy milk",  "done" => false),
        Dict("title" => "ship v1.0", "done" => true),
    ])
    println("session 1: wrote $(count_docs(db, "notes")) notes, closing")
    close(db)
end

# Session 2 — reopen the same path; the data survived the close.
let db = open_db(path)
    println("session 2: reopened $path")
    for n in find(db, "notes", Dict())
        println("  [", n["done"] ? "x" : " ", "] ", n["title"])
    end
    close(db)
end
