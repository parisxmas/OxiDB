#!/usr/bin/env julia
# ──────────────────────────────────────────────────────────────────────
# SQLite vs OxiDB — apples-to-apples in-process benchmark
# ──────────────────────────────────────────────────────────────────────
#
# Methodology
# -----------
# * Both engines run in-process on local files (no server, no network).
# * Same dataset (N documents, 7 fields each), same RNG seed, fresh
#   storage for every run.
# * Both engines run with their default durability settings — strict
#   per-commit fsync (SQLite: `synchronous=FULL`, journal_mode=DELETE;
#   OxiDB: `OXIDB_LAZY_SYNC` off).
# * SQLite uses prepared statements and wraps the bulk insert in a
#   transaction — the *idiomatic* fast path. OxiDB uses `insert_many`
#   (the idiomatic batched insert). This is an API-vs-API comparison,
#   not a wire-protocol microbench.
# * For each operation: 1 warmup run (discarded), then RUNS measured
#   runs. We report median, min, max, and stddev across the measured
#   runs.
#
# Tunables
# --------
#   OXIDB_BENCH_N      docs in the dataset (default 100_000)
#   OXIDB_BENCH_RUNS   measured runs per operation (default 5)
#
# Output is a Markdown table on stdout.
# ──────────────────────────────────────────────────────────────────────

using OxiDbEmbedded
using SQLite, DBInterface
using Random, Statistics, Printf

# ── Configuration ─────────────────────────────────────────────────────
const N           = parse(Int, get(ENV, "OXIDB_BENCH_N",    "100000"))
const RUNS        = parse(Int, get(ENV, "OXIDB_BENCH_RUNS", "5"))
const N_LOOKUPS   = 10_000     # point lookups in the lookup bench
const N_RANGE     = 100        # range-query iterations
const N_AGG       = 100        # aggregate iterations
const N_UPDATES   = 10_000     # update-by-id iterations
const SEED        = 0xC0FFEE

# ── Dataset (deterministic) ───────────────────────────────────────────
const CITIES = ["NYC", "LDN", "TYO", "BER", "PAR", "SYD", "TOR", "MUM"]

function gen_data(n::Int)
    rng = MersenneTwister(SEED)
    [Dict(
        "id"         => i,
        "name"       => "user_$(i)",
        "email"      => "user.$(i)@bench.test",
        "age"        => 18 + rand(rng, 0:62),
        "score"      => round(rand(rng) * 100; digits=2),
        "city"       => CITIES[rand(rng, 1:length(CITIES))],
        "active"     => rand(rng, Bool),
    ) for i in 1:n]
end

# ── Timing harness ────────────────────────────────────────────────────
"""
    bench(name; warmup, runs) do; ...; end -> (median_s, samples)

Run the block `warmup` times (discarded) then `runs` times (measured).
Returns the median in seconds and the raw samples.
"""
function bench(body; warmup=1, runs=RUNS)
    for _ in 1:warmup; body(); end
    samples = Float64[]
    for _ in 1:runs
        t = @elapsed body()
        push!(samples, t)
    end
    samples
end

stats(xs) = (median = median(xs),
             mn     = minimum(xs),
             mx     = maximum(xs),
             sd     = length(xs) > 1 ? std(xs) : 0.0)

fmt_ms(s) = s >= 1.0 ? @sprintf("%.0f", s*1000) : @sprintf("%.2f", s*1000)

# ── SQLite helpers ────────────────────────────────────────────────────
"""
Run a setup/DDL statement and ensure its prepared statement is finalized.
SQLite refuses to start a transaction if any statement is still "in progress",
so we explicitly close every cursor returned during setup.
"""
function _ddl!(db, sql::AbstractString)
    cur = DBInterface.execute(db, sql)
    for _ in cur; end                       # consume any PRAGMA / pragma-style rows
    try DBInterface.close!(cur) catch; end  # finalize the underlying statement
end

function sqlite_open(path::String)
    db = SQLite.DB(path)
    _ddl!(db, "PRAGMA journal_mode = DELETE")    # default
    _ddl!(db, "PRAGMA synchronous  = FULL")      # default
    _ddl!(db, """
        CREATE TABLE people (
            id     INTEGER PRIMARY KEY,
            name   TEXT NOT NULL,
            email  TEXT NOT NULL UNIQUE,
            age    INTEGER,
            score  REAL,
            city   TEXT,
            active INTEGER
        )
    """)
    _ddl!(db, "CREATE INDEX idx_age  ON people(age)")
    _ddl!(db, "CREATE INDEX idx_city ON people(city)")
    db
end

function sqlite_bulk_insert!(db, rows)
    SQLite.transaction(db) do
        stmt = DBInterface.prepare(db,
            "INSERT INTO people (id,name,email,age,score,city,active) VALUES (?,?,?,?,?,?,?)")
        for r in rows
            DBInterface.execute(stmt, (r["id"], r["name"], r["email"],
                                       r["age"], r["score"], r["city"],
                                       r["active"] ? 1 : 0))
        end
        DBInterface.close!(stmt)
    end
end

# ── OxiDB helpers ─────────────────────────────────────────────────────
function oxidb_open(path::String)
    db = open_db(path)
    create_unique_index(db, "people", "email")
    create_index(db, "people", "age")
    create_index(db, "people", "city")
    create_index(db, "people", "id")          # for fair point-lookup-by-id
    db
end

oxidb_bulk_insert!(db, rows) = insert_many(db, "people", rows)

# ── Per-engine benchmark drivers ──────────────────────────────────────
function bench_sqlite(rows)
    results = Dict{String, Vector{Float64}}()

    # 1. Bulk insert (fresh DB each run)
    results["Bulk insert $N"] = let s = Float64[]
        for _ in 1:RUNS + 1
            path = tempname() * ".db"
            db = sqlite_open(path)
            t = @elapsed sqlite_bulk_insert!(db, rows)
            DBInterface.close!(db); rm(path; force=true)
            push!(s, t)
        end
        s[2:end]                                       # drop warmup
    end

    # Seed a single DB for the remaining benches
    path = tempname() * ".db"
    db = sqlite_open(path)
    sqlite_bulk_insert!(db, rows)

    # 2. Point lookup by indexed UNIQUE email
    emails = ["user.$(rand(1:N))@bench.test" for _ in 1:N_LOOKUPS]
    results["Point lookup (indexed)"] = bench(warmup=1, runs=RUNS) do
        stmt = DBInterface.prepare(db, "SELECT * FROM people WHERE email = ?")
        for e in emails
            for _ in DBInterface.execute(stmt, (e,)); end
        end
        DBInterface.close!(stmt)
    end

    # 3. Range query on indexed age
    results["Range query (indexed)"] = bench(warmup=1, runs=RUNS) do
        for _ in 1:N_RANGE
            for _ in DBInterface.execute(db,
                "SELECT * FROM people WHERE age >= 30 AND age < 40"); end
        end
    end

    # 4. Aggregation: group by city, count + avg age
    results["Aggregate (group + avg)"] = bench(warmup=1, runs=RUNS) do
        for _ in 1:N_AGG
            for _ in DBInterface.execute(db,
                "SELECT city, COUNT(*) AS n, AVG(age) AS avg_age FROM people GROUP BY city"); end
        end
    end

    # 5a. Update by indexed id ($inc-equivalent), auto-commit per stmt
    ids = rand(1:N, N_UPDATES)
    results["Update by id (auto-commit)"] = bench(warmup=1, runs=RUNS) do
        stmt = DBInterface.prepare(db, "UPDATE people SET age = age + 1 WHERE id = ?")
        for i in ids
            DBInterface.execute(stmt, (i,))
        end
        DBInterface.close!(stmt)
    end

    # 5b. Same updates wrapped in ONE transaction — collapses N fsyncs to 1
    results["Update by id (1 tx)"] = bench(warmup=1, runs=RUNS) do
        SQLite.transaction(db) do
            stmt = DBInterface.prepare(db, "UPDATE people SET age = age + 1 WHERE id = ?")
            for i in ids
                DBInterface.execute(stmt, (i,))
            end
            DBInterface.close!(stmt)
        end
    end

    DBInterface.close!(db); rm(path; force=true)
    results
end

function bench_oxidb(rows)
    results = Dict{String, Vector{Float64}}()

    # 1. Bulk insert (fresh dir each run)
    results["Bulk insert $N"] = let s = Float64[]
        for _ in 1:RUNS + 1
            dir = mktempdir()
            db = oxidb_open(dir)
            t = @elapsed oxidb_bulk_insert!(db, rows)
            close(db); rm(dir; recursive=true, force=true)
            push!(s, t)
        end
        s[2:end]
    end

    # Seed one DB for the remaining benches
    dir = mktempdir()
    db = oxidb_open(dir)
    oxidb_bulk_insert!(db, rows)

    # 2. Point lookup by indexed UNIQUE email
    emails = ["user.$(rand(1:N))@bench.test" for _ in 1:N_LOOKUPS]
    results["Point lookup (indexed)"] = bench(warmup=1, runs=RUNS) do
        for e in emails
            find_one(db, "people", Dict("email" => e))
        end
    end

    # 3. Range query on indexed age
    results["Range query (indexed)"] = bench(warmup=1, runs=RUNS) do
        for _ in 1:N_RANGE
            find(db, "people",
                 Dict("age" => Dict("\$gte" => 30, "\$lt" => 40)))
        end
    end

    # 4. Aggregation: group by city, count + avg age
    pipeline = [
        Dict("\$group" => Dict("_id"     => "\$city",
                               "n"       => Dict("\$sum" => 1),
                               "avg_age" => Dict("\$avg" => "\$age"))),
    ]
    results["Aggregate (group + avg)"] = bench(warmup=1, runs=RUNS) do
        for _ in 1:N_AGG
            aggregate(db, "people", pipeline)
        end
    end

    # 5a. Update by indexed id ($inc), auto-commit per call
    ids = rand(1:N, N_UPDATES)
    results["Update by id (auto-commit)"] = bench(warmup=1, runs=RUNS) do
        for i in ids
            update(db, "people", Dict("id" => i),
                   Dict("\$inc" => Dict("age" => 1)))
        end
    end

    # 5b. Same updates wrapped in ONE transaction — engine collapses
    #     N WAL fsyncs to 1 via the committer-loop batched commit.
    results["Update by id (1 tx)"] = bench(warmup=1, runs=RUNS) do
        transaction(db) do
            for i in ids
                update(db, "people", Dict("id" => i),
                       Dict("\$inc" => Dict("age" => 1)))
            end
        end
    end

    close(db); rm(dir; recursive=true, force=true)
    results
end

# ── Run + report ──────────────────────────────────────────────────────
println("# SQLite vs OxiDB — in-process embedded benchmark\n")
println("Methodology: each engine runs in-process on local files at its default ")
println("durability settings (per-commit fsync). Same dataset, same RNG seed, ")
println("fresh storage for every run. 1 warmup + $RUNS measured runs per op.\n")

println("Hardware: ", Sys.MACHINE, "    Julia: ", VERSION)
println("Dataset:  ", N, " docs, 7 fields each (id, name, email, age, score, city, active)")
sqlite_ver = try
    string(SQLite.SQLiteCom.libsqliteversion())
catch
    isdefined(SQLite, :sqlitever) ? string(SQLite.sqlitever()) : "(version probe unsupported)"
end
println("SQLite:   ", sqlite_ver, "    SQLite.jl from registry")
println("OxiDB:    OxiDbEmbedded v0.6.0, native lib from julia/OxiDbEmbedded/lib/\n")

println("Generating dataset…")
rows = gen_data(N)

println("Running SQLite…")
sql_res = bench_sqlite(rows)
println("Running OxiDB…")
oxi_res = bench_oxidb(rows)

println("\n## Results")
println("Median ± stddev, milliseconds (lower is better). Ratio = OxiDB / SQLite.\n")
println("| Operation | SQLite | OxiDB | Ratio |")
println("|-----------|--------|-------|-------|")
ops = ["Bulk insert $N", "Point lookup (indexed)", "Range query (indexed)",
       "Aggregate (group + avg)",
       "Update by id (auto-commit)", "Update by id (1 tx)"]
for op in ops
    s = stats(sql_res[op]);  o = stats(oxi_res[op])
    ratio = o.median / s.median
    arrow = ratio < 1.0 ? @sprintf("%.2fx (OxiDB wins)", 1/ratio) :
                          @sprintf("%.2fx", ratio)
    @printf("| %-23s | %5s ± %4s ms | %5s ± %4s ms | %s |\n",
            op, fmt_ms(s.median), fmt_ms(s.sd),
                fmt_ms(o.median), fmt_ms(o.sd), arrow)
end

println("\n## Raw samples (ms)")
for op in ops
    @printf("%-23s  SQLite: %s\n", op,
            join(map(x -> fmt_ms(x), sql_res[op]), ", "))
    @printf("%-23s   OxiDB: %s\n", "",
            join(map(x -> fmt_ms(x), oxi_res[op]), ", "))
end
