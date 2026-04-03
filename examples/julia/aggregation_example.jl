#!/usr/bin/env julia
#
# OxiDB Julia Example — Aggregation Pipeline
#
# Demonstrates the full aggregation pipeline with real-world scenarios.
#
# Prerequisites:
#   1. Start oxidb-server on 127.0.0.1:4444
#   2. Run: cd examples/julia && julia --project=. aggregation_example.jl
#

using OxiDb

function main()
    client = connect_oxidb("127.0.0.1", 4444)
    println("Connected to OxiDB.\n")

    # Clean up
    for col in ["sales", "customers"]
        try; drop_collection(client, col); catch; end
    end

    # ──────────────────────────────────────────────────────────────
    # Seed data — sales records
    # ──────────────────────────────────────────────────────────────
    println("Seeding data...")

    customers = [
        Dict("customer_id" => "C1", "name" => "Alice", "region" => "US", "tier" => "gold"),
        Dict("customer_id" => "C2", "name" => "Bob",   "region" => "EU", "tier" => "silver"),
        Dict("customer_id" => "C3", "name" => "Carol", "region" => "US", "tier" => "gold"),
        Dict("customer_id" => "C4", "name" => "Dave",  "region" => "EU", "tier" => "bronze"),
    ]
    insert_many(client, "customers", customers)

    sales = [
        Dict("customer_id" => "C1", "product" => "Laptop",   "category" => "Electronics", "amount" => 999, "qty" => 1),
        Dict("customer_id" => "C1", "product" => "Mouse",    "category" => "Electronics", "amount" => 29,  "qty" => 3),
        Dict("customer_id" => "C2", "product" => "Laptop",   "category" => "Electronics", "amount" => 999, "qty" => 2),
        Dict("customer_id" => "C2", "product" => "Keyboard", "category" => "Electronics", "amount" => 79,  "qty" => 1),
        Dict("customer_id" => "C3", "product" => "Book",     "category" => "Books",       "amount" => 15,  "qty" => 5),
        Dict("customer_id" => "C3", "product" => "Laptop",   "category" => "Electronics", "amount" => 999, "qty" => 1),
        Dict("customer_id" => "C4", "product" => "Mouse",    "category" => "Electronics", "amount" => 29,  "qty" => 10),
        Dict("customer_id" => "C4", "product" => "Book",     "category" => "Books",       "amount" => 15,  "qty" => 2),
    ]
    insert_many(client, "sales", sales)

    create_index(client, "sales", "customer_id")
    create_index(client, "sales", "category")
    println("   $(length(customers)) customers, $(length(sales)) sales records\n")

    # ──────────────────────────────────────────────────────────────
    # 1. $group — revenue by category
    # ──────────────────────────────────────────────────────────────
    println("1. Revenue by category")
    results = aggregate(client, "sales", [
        Dict("\$group" => Dict(
            "_id" => "\$category",
            "total_revenue" => Dict("\$sum" => "\$amount"),
            "total_qty" => Dict("\$sum" => "\$qty"),
            "order_count" => Dict("\$sum" => 1)
        )),
        Dict("\$sort" => Dict("total_revenue" => -1))
    ])
    for r in results
        println("   $(r["_id"]): \$$(r["total_revenue"]) ($(r["total_qty"]) items, $(r["order_count"]) orders)")
    end

    # ──────────────────────────────────────────────────────────────
    # 2. $match + $group — electronics sales by product
    # ──────────────────────────────────────────────────────────────
    println("\n2. Electronics sales by product")
    results = aggregate(client, "sales", [
        Dict("\$match" => Dict("category" => "Electronics")),
        Dict("\$group" => Dict(
            "_id" => "\$product",
            "revenue" => Dict("\$sum" => "\$amount"),
            "units" => Dict("\$sum" => "\$qty")
        )),
        Dict("\$sort" => Dict("revenue" => -1))
    ])
    for r in results
        println("   $(r["_id"]): \$$(r["revenue"]) ($(r["units"]) units)")
    end

    # ──────────────────────────────────────────────────────────────
    # 3. $group + $count — customer with most orders
    # ──────────────────────────────────────────────────────────────
    println("\n3. Orders per customer (top spenders)")
    results = aggregate(client, "sales", [
        Dict("\$group" => Dict(
            "_id" => "\$customer_id",
            "total_spent" => Dict("\$sum" => "\$amount"),
            "orders" => Dict("\$sum" => 1)
        )),
        Dict("\$sort" => Dict("total_spent" => -1)),
        Dict("\$limit" => 3)
    ])
    for r in results
        println("   $(r["_id"]): \$$(r["total_spent"]) ($(r["orders"]) orders)")
    end

    # ──────────────────────────────────────────────────────────────
    # 4. $project — reshape output
    # ──────────────────────────────────────────────────────────────
    println("\n4. \$project — custom output fields")
    results = aggregate(client, "sales", [
        Dict("\$match" => Dict("product" => "Laptop")),
        Dict("\$project" => Dict(
            "_id" => 0,
            "customer" => "\$customer_id",
            "product" => 1,
            "total" => Dict("\$multiply" => ["\$amount", "\$qty"])
        ))
    ])
    for r in results
        println("   $(r["customer"]): $(r["product"]) = \$$(r["total"])")
    end

    # ──────────────────────────────────────────────────────────────
    # 5. $lookup — join sales with customers
    # ──────────────────────────────────────────────────────────────
    println("\n5. \$lookup — join sales with customers")
    results = aggregate(client, "sales", [
        Dict("\$match" => Dict("product" => "Laptop")),
        Dict("\$lookup" => Dict(
            "from" => "customers",
            "localField" => "customer_id",
            "foreignField" => "customer_id",
            "as" => "customer_info"
        )),
        Dict("\$project" => Dict(
            "_id" => 0,
            "product" => 1,
            "amount" => 1,
            "customer_name" => Dict("\$first" => "\$customer_info.name"),
            "region" => Dict("\$first" => "\$customer_info.region")
        ))
    ])
    for r in results
        name = get(r, "customer_name", "?")
        region = get(r, "region", "?")
        println("   $name ($region) bought $(r["product"]) for \$$(r["amount"])")
    end

    # ──────────────────────────────────────────────────────────────
    # 6. $addFields + $sort — computed fields
    # ──────────────────────────────────────────────────────────────
    println("\n6. \$addFields — computed line total")
    results = aggregate(client, "sales", [
        Dict("\$addFields" => Dict(
            "line_total" => Dict("\$multiply" => ["\$amount", "\$qty"])
        )),
        Dict("\$sort" => Dict("line_total" => -1)),
        Dict("\$limit" => 5),
        Dict("\$project" => Dict("_id" => 0, "product" => 1, "qty" => 1, "amount" => 1, "line_total" => 1))
    ])
    for r in results
        println("   $(r["product"]): $(r["qty"]) x \$$(r["amount"]) = \$$(r["line_total"])")
    end

    # ──────────────────────────────────────────────────────────────
    # 7. $skip + $limit — pagination
    # ──────────────────────────────────────────────────────────────
    println("\n7. Pagination: page 1 (3 items) and page 2")
    page1 = aggregate(client, "sales", [
        Dict("\$sort" => Dict("amount" => -1)),
        Dict("\$skip" => 0),
        Dict("\$limit" => 3),
        Dict("\$project" => Dict("_id" => 0, "product" => 1, "amount" => 1))
    ])
    println("   Page 1: $(map(r -> "$(r["product"])(\$$(r["amount"]))", page1))")

    page2 = aggregate(client, "sales", [
        Dict("\$sort" => Dict("amount" => -1)),
        Dict("\$skip" => 3),
        Dict("\$limit" => 3),
        Dict("\$project" => Dict("_id" => 0, "product" => 1, "amount" => 1))
    ])
    println("   Page 2: $(map(r -> "$(r["product"])(\$$(r["amount"]))", page2))")

    close(client)
    println("\nDone!")
end

main()
