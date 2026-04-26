#!/usr/bin/env julia
# 07_vector_search.jl — RAG-style nearest-neighbor over embeddings.
# Stand-in 8-dim embeddings (use OpenAI / sentence-transformers in real apps).

using OxiDb
using Random
using LinearAlgebra
Random.seed!(7)

# Toy "embedding" — random unit vector. Replace with a real model.
embed(text) = (v = randn(8); v / norm(v))

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "kb", Dict{String,Any}())

    chunks = [
        ("py-001", "Python uses indentation to define code blocks."),
        ("jl-001", "Julia ships with a JIT compiler based on LLVM."),
        ("rs-001", "Rust enforces ownership at compile time."),
        ("jl-002", "Julia's multiple dispatch is its defining feature."),
    ]
    for (id, text) in chunks
        insert(db, "kb", Dict("id" => id, "text" => text, "vec" => embed(text)))
    end

    exec(db, "create_vector_index"; collection = "kb",
                                    field      = "vec",
                                    dimension  => 8,
                                    metric     = "cosine")

    query_vec = embed("what makes julia unique")
    hits = exec(db, "vector_search"; collection = "kb",
                                     field      = "vec",
                                     vector     = query_vec,
                                     limit      = 3)
    println("Top-3 matches:")
    foreach(h -> println("  ", round(h["score"], digits=3), "  ", h["doc"]["text"]), hits)
end
