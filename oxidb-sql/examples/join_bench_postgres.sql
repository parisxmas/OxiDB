-- PostgreSQL side of the oxidb-sql hard-join benchmark.
-- Produces byte-identical data to examples/join_bench.rs (same formulas), so
-- the two engines can be compared for both correctness and speed.
--
-- Repro:
--   createdb oxidb_sql_bench
--   psql -d oxidb_sql_bench -f join_bench_postgres.sql
--
-- Sizes: R=10 regions, S=10 suppliers, C=1000 customers, P=300 products,
--        O=5000 orders, I=15000 items. i is 0-based, id = i+1.

DROP TABLE IF EXISTS items, orders, products, customers, suppliers, regions;

CREATE TABLE regions   (id INT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, region_id INT);
CREATE TABLE products  (id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL, supplier_id INT);
CREATE TABLE orders    (id INT PRIMARY KEY, customer_id INT NOT NULL, total INT);
CREATE TABLE items     (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT);

INSERT INTO regions   SELECT i+1, 'R'||(i+1) FROM generate_series(0, 9) i;
INSERT INTO suppliers SELECT i+1, 'S'||(i+1) FROM generate_series(0, 9) i;
INSERT INTO customers
  SELECT i+1, 'c'||(i+1), CASE WHEN i%7=0 THEN NULL ELSE (i%10)+1 END
  FROM generate_series(0, 999) i;
INSERT INTO products
  SELECT i+1, 'p'||(i+1), (i%9)+1, CASE WHEN i%5=0 THEN NULL ELSE (i%10)+1 END
  FROM generate_series(0, 299) i;
INSERT INTO orders
  SELECT i+1, CASE WHEN i%11=0 THEN 1000000+i ELSE (i%1000)+1 END, i%100
  FROM generate_series(0, 4999) i;
INSERT INTO items
  SELECT i+1, (i%5000)+1, (i%300)+1, (i%5)+1
  FROM generate_series(0, 14999) i;
ANALYZE;

\timing on

-- Q1: 5-way INNER — revenue per region
SELECT r.name, SUM(it.qty*p.price) AS rev
FROM regions r
JOIN customers c ON c.region_id=r.id
JOIN orders o    ON o.customer_id=c.id
JOIN items it    ON it.order_id=o.id
JOIN products p  ON p.id=it.product_id
GROUP BY r.name ORDER BY r.name;

-- Q2: 6-way INNER — revenue per supplier
SELECT s.name, SUM(it.qty*p.price) AS rev
FROM regions r
JOIN customers c ON c.region_id=r.id
JOIN orders o    ON o.customer_id=c.id
JOIN items it    ON it.order_id=o.id
JOIN products p  ON p.id=it.product_id
JOIN suppliers s ON s.id=p.supplier_id
GROUP BY s.name ORDER BY s.name;

-- Q3: LEFT chain — order count per region
SELECT r.name, COUNT(o.id) AS n
FROM regions r
LEFT JOIN customers c ON c.region_id=r.id
LEFT JOIN orders o    ON o.customer_id=c.id
GROUP BY r.name ORDER BY r.name;

-- Q4: FULL join — row count
SELECT COUNT(*) AS n FROM customers c FULL JOIN orders o ON c.id=o.customer_id;
