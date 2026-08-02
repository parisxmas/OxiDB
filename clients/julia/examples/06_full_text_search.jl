#!/usr/bin/env julia
# 06_full_text_search.jl — TF-IDF ranked text search over articles.
# Build a text index on `title` + `body`, then search across both.

using OxiDb

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "articles", Dict{String,Any}())

    insert_many(db, "articles", [
        Dict("title" => "Getting started with Julia",
             "body"  => "Julia is a fast, dynamic language for technical computing."),
        Dict("title" => "Pandas vs DataFrames.jl",
             "body"  => "DataFrames.jl is the Julia answer to pandas — columnar, typed, fast."),
        Dict("title" => "Why Rust is taking over systems programming",
             "body"  => "Rust offers memory safety without a garbage collector."),
        Dict("title" => "Julia for machine learning",
             "body"  => "Flux.jl and MLJ give Julia a competitive ML stack."),
    ])

    exec(db, "create_text_index"; collection = "articles",
                                  fields     = ["title", "body"])

    hits = exec(db, "text_search"; collection = "articles",
                                   query      = "julia ml",
                                   limit      = 5)
    println("TF-IDF results for 'julia ml':")
    foreach(h -> println("  ", round(h["_score"], digits=3), "  ", h["title"]), hits)
end
