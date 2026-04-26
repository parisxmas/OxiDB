#!/usr/bin/env julia
# 04_atomic_counter_and_push.jl — like counter + tag append in one call.
# Combine $inc and $push so OCC validates a single document version.

using OxiDb

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "posts", Dict{String,Any}())

    insert(db, "posts", Dict(
        "slug"  => "intro-to-julia",
        "title" => "Intro to Julia",
        "likes" => 0,
        "tags"  => ["beginner"],
    ))

    @info "User 'jane' likes the post and adds two tags"
    update(db, "posts",
        Dict("slug" => "intro-to-julia"),
        Dict(
            "\$inc" => Dict("likes" => 1),
            "\$push"     => Dict("tags" => "data-science"),
            "\$addToSet" => Dict("tags" => "tutorial"),
        ),
    )

    p = find_one(db, "posts", Dict("slug" => "intro-to-julia"))
    println("likes = ", p["likes"], "  tags = ", p["tags"])
end
