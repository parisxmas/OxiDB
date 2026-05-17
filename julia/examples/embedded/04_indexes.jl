#!/usr/bin/env julia
# 04_indexes.jl — a secondary index speeds up lookups on a field; a unique
# index additionally rejects duplicates at insert time.

using OxiDbEmbedded

db = open_db(mktempdir())
insert_many(db, "users", [
    Dict("name" => "Alice", "email" => "alice@example.com", "city" => "NYC"),
    Dict("name" => "Bob",   "email" => "bob@example.com",   "city" => "LDN"),
    Dict("name" => "Carol", "email" => "carol@example.com", "city" => "NYC"),
])

create_index(db, "users", "city")
create_unique_index(db, "users", "email")
for ix in list_indexes(db, "users")
    println("  index '", ix["name"], "' on ", join(ix["fields"], ", "), ix["unique"] ? " (unique)" : "")
end

println("city = NYC -> ", [u["name"] for u in find(db, "users", Dict("city" => "NYC"))])

# The unique index on email rejects a duplicate.
try
    insert(db, "users", Dict("name" => "Eve", "email" => "alice@example.com"))
    println("ERROR: duplicate email should have been rejected")
catch e
    println("unique index rejected duplicate: ", e isa OxiDbError ? e.msg : sprint(showerror, e))
end

close(db)
