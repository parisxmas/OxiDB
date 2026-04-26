#!/usr/bin/env julia
# 09_ttl_sessions.jl — sessions that auto-expire 1 hour after creation.
# OxiDB's TTL index sweeps and deletes expired rows in the background.

using OxiDb

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "sessions", Dict{String,Any}())

    # Create the TTL index once. expireAfterSeconds is relative to the
    # value of `created_at` on each document.
    exec(db, "create_ttl_index";
         collection         = "sessions",
         field              = "created_at",
         expireAfterSeconds = 3600)

    # Modern Julia: System time as epoch seconds → ISO 8601-friendly date string.
    now_ms() = round(Int, time() * 1000)

    for user in ["alice", "bob", "carol"]
        insert(db, "sessions", Dict(
            "user_id"    => user,
            "token"      => bytes2hex(rand(UInt8, 16)),
            "ip"         => "10.0.0.$(rand(1:254))",
            "created_at" => now_ms(),
        ))
    end

    println("active sessions: ", count_docs(db, "sessions"))
    for s in find(db, "sessions"; sort = Dict("created_at" => -1))
        println("  ", s["user_id"], "  expires @ ", s["created_at"] + 3600 * 1000)
    end
end
