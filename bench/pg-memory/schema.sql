-- Shared schema for the OxiDB / PostgreSQL memory benchmark.
--
-- Deliberately not a toy: five related tables, 1,000,000 rows in total, with
-- every index shape that costs an engine memory — a surrogate primary key, a
-- composite (multi-column) primary key, enforced single-column foreign keys,
-- unique columns, single-column indexes and multi-column ones.
--
-- Written to the intersection of both engines so one file loads into both:
-- column-level UNIQUE (OxiDB does not enforce table-level or CREATE UNIQUE
-- INDEX), single-column FOREIGN KEY (OxiDB does not enforce composite FKs),
-- and no types either side would have to emulate.

CREATE TABLE customers (
  id       INT PRIMARY KEY,
  email    TEXT UNIQUE,
  name     TEXT NOT NULL,
  country  TEXT NOT NULL,
  created  TIMESTAMP
);
CREATE INDEX idx_customers_country ON customers (country);
CREATE INDEX idx_customers_geo ON customers (country, created);

CREATE TABLE products (
  id        INT PRIMARY KEY,
  sku       TEXT UNIQUE,
  category  TEXT NOT NULL,
  price     DOUBLE PRECISION NOT NULL,
  active    BOOLEAN NOT NULL
);
CREATE INDEX idx_products_category ON products (category);
CREATE INDEX idx_products_cat_price ON products (category, price);

CREATE TABLE orders (
  id           INT PRIMARY KEY,
  customer_id  INT NOT NULL REFERENCES customers (id),
  status       TEXT NOT NULL,
  total        DOUBLE PRECISION NOT NULL,
  created      TIMESTAMP
);
CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_orders_status_created ON orders (status, created);

-- Composite PRIMARY KEY: the key map is a tuple map on both engines.
CREATE TABLE order_items (
  order_id  INT NOT NULL REFERENCES orders (id),
  line_no   INT NOT NULL,
  product   INT NOT NULL,
  qty       INT NOT NULL,
  amount    DOUBLE PRECISION NOT NULL,
  CONSTRAINT pk_order_items PRIMARY KEY (order_id, line_no)
);
CREATE INDEX idx_items_product ON order_items (product);

-- Composite PRIMARY KEY again, plus a foreign key on its first column.
CREATE TABLE inventory (
  product_id  INT NOT NULL REFERENCES products (id),
  warehouse   TEXT NOT NULL,
  on_hand     INT NOT NULL,
  reorder_at  INT NOT NULL,
  CONSTRAINT pk_inventory PRIMARY KEY (product_id, warehouse)
);
CREATE INDEX idx_inventory_warehouse ON inventory (warehouse);
