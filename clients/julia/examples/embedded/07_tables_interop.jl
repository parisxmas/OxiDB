#!/usr/bin/env julia
# 07_tables_interop.jl — query results satisfy the Tables.jl interface, so
# they flow straight into DataFrames, CSV, MLJ, GLM, Plots, …
#
# Here we use Tables.jl directly to stay zero-dep; in real code you'd most
# often write `DataFrame(rows)`.

using OxiDbEmbedded
using Tables

db = open_db(mktempdir())
insert_many(db, "people", [
    Dict("name" => "Alice", "age" => 30, "city" => "NYC"),
    Dict("name" => "Bob",   "age" => 25),                    # heterogeneous — no city
    Dict("name" => "Carol", "age" => 42, "city" => "LDN"),
])

rows = find(db, "people", Dict())

# `rows` walks like a Vector{Dict} — old code keeps working...
println("length    = ", length(rows))
println("rows[1]   = ", rows[1])

# ...and it's a Tables.jl table — `DataFrame(rows)`, `CSV.write(io, rows)`,
# any Tables.jl consumer accepts it directly.
println("istable   = ", Tables.istable(rows))

cols = Tables.columntable(rows)        # column-major NamedTuple of Vectors
println("columns   = ", keys(cols))
println("names     = ", cols.name)
println("ages      = ", cols.age)
println("cities    = ", cols.city)     # Union{Missing,String} — Bob's city is `missing`

close(db)
