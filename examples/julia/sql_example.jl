#!/usr/bin/env julia
#
# OxiDB Julia Example — SQL Queries
#
# Demonstrates the SQL query interface alongside JSON queries.
#
# Prerequisites:
#   1. Start oxidb-server on 127.0.0.1:4444
#   2. Run: cd examples/julia && julia --project=. sql_example.jl
#

using OxiDb

function main()
    client = connect_oxidb("127.0.0.1", 4444)
    println("Connected to OxiDB.\n")

    # Clean up
    for col in ["employees", "departments"]
        try; drop_collection(client, col); catch; end
    end

    # ──────────────────────────────────────────────────────────────
    # 1. INSERT via SQL
    # ──────────────────────────────────────────────────────────────
    println("1. INSERT via SQL")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Alice', 'Engineering', 95000, 32)")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Bob', 'Engineering', 88000, 28)")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Carol', 'Marketing', 72000, 35)")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Dave', 'Marketing', 68000, 29)")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Eve', 'Sales', 78000, 31)")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Frank', 'Sales', 82000, 40)")
    sql(client, "INSERT INTO employees (name, dept, salary, age) VALUES ('Grace', 'Engineering', 105000, 38)")
    println("   Inserted 7 employees")

    # ──────────────────────────────────────────────────────────────
    # 2. SELECT queries
    # ──────────────────────────────────────────────────────────────
    println("\n2. SELECT queries")

    rows = sql(client, "SELECT * FROM employees WHERE dept = 'Engineering' ORDER BY salary DESC")
    println("   Engineering team (by salary desc):")
    for r in rows
        println("     $(r["name"]): \$$(r["salary"])")
    end

    rows = sql(client, "SELECT * FROM employees WHERE salary > 80000 AND age < 35")
    println("   High earners under 35:")
    for r in rows
        println("     $(r["name"]) ($(r["dept"])): \$$(r["salary"]), age $(r["age"])")
    end

    rows = sql(client, "SELECT * FROM employees WHERE name LIKE 'A%' OR name LIKE 'E%'")
    println("   Names starting with A or E:")
    for r in rows
        println("     $(r["name"])")
    end

    # ──────────────────────────────────────────────────────────────
    # 3. Aggregate functions
    # ──────────────────────────────────────────────────────────────
    println("\n3. Aggregate functions")

    rows = sql(client, "SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_salary FROM employees GROUP BY dept")
    println("   Department stats:")
    for r in rows
        avg = round(r["avg_salary"], digits=0)
        println("     $(r["dept"]): $(r["cnt"]) people, avg \$$avg")
    end

    rows = sql(client, "SELECT dept, MAX(salary) as top FROM employees GROUP BY dept HAVING MAX(salary) > 80000")
    println("   Departments with top salary > 80K:")
    for r in rows
        println("     $(r["dept"]): \$$(r["top"])")
    end

    # ──────────────────────────────────────────────────────────────
    # 4. UPDATE and DELETE via SQL
    # ──────────────────────────────────────────────────────────────
    println("\n4. UPDATE and DELETE")

    sql(client, "UPDATE employees SET salary = 100000 WHERE name = 'Bob'")
    bob = sql(client, "SELECT name, salary FROM employees WHERE name = 'Bob'")
    println("   Bob's new salary: \$$(bob[1]["salary"])")

    sql(client, "DELETE FROM employees WHERE name = 'Dave'")
    rows = sql(client, "SELECT name FROM employees")
    println("   After deleting Dave: $(length(rows)) employees remain")

    # ──────────────────────────────────────────────────────────────
    # 5. CREATE INDEX via SQL
    # ──────────────────────────────────────────────────────────────
    println("\n5. Indexes via SQL")
    sql(client, "CREATE INDEX idx_dept ON employees (dept)")
    sql(client, "CREATE INDEX idx_salary ON employees (salary)")
    println("   Created indexes on dept and salary")

    indexes = list_indexes(client, "employees")
    for idx in indexes
        println("   Index: $(idx["name"]) ($(idx["index_type"]))")
    end

    # ──────────────────────────────────────────────────────────────
    # 6. TTL INDEX via SQL
    # ──────────────────────────────────────────────────────────────
    println("\n6. TTL index via SQL")
    try; drop_collection(client, "audit_log"); catch; end
    sql(client, "CREATE TTL INDEX ON audit_log (timestamp) EXPIRE AFTER 2592000")
    println("   Created TTL index on audit_log (30-day expiry)")

    indexes = list_indexes(client, "audit_log")
    for idx in indexes
        if idx["index_type"] == "ttl"
            println("   TTL: $(idx["name"]), expire=$(idx["expire_after_seconds"])s")
        end
    end

    # ──────────────────────────────────────────────────────────────
    # 7. SHOW TABLES
    # ──────────────────────────────────────────────────────────────
    println("\n7. SHOW TABLES")
    tables = sql(client, "SHOW TABLES")
    println("   Tables: $tables")

    # ──────────────────────────────────────────────────────────────
    # 8. Mix SQL and JSON queries
    # ──────────────────────────────────────────────────────────────
    println("\n8. SQL + JSON on the same collection")

    # Insert via JSON
    insert(client, "employees", Dict("name" => "Hank", "dept" => "Engineering", "salary" => 92000, "age" => 27))

    # Query via SQL
    eng = sql(client, "SELECT name, salary FROM employees WHERE dept = 'Engineering' ORDER BY salary DESC")
    println("   Engineering (SQL):")
    for r in eng
        println("     $(r["name"]): \$$(r["salary"])")
    end

    # Query same data via JSON
    eng2 = find(client, "employees", Dict("dept" => "Engineering");
                sort=Dict("salary" => -1))
    println("   Engineering (JSON):")
    for r in eng2
        println("     $(r["name"]): \$$(r["salary"])")
    end

    close(client)
    println("\nDone!")
end

main()
