CREATE TABLE IF NOT EXISTS employees (

    id INTEGER PRIMARY KEY,
    full_name TEXT,
    age INTEGER,
    email TEXT UNIQUE,
    department TEXT,
    salary REAL,
    join_date TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS departments (
    dept_id TEXT PRIMARY KEY,
    dept_name TEXT,
    manager TEXT,
    location TEXT,
    budget REAL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    email TEXT,
    country TEXT,
    city TEXT,
    loyalty_tier TEXT,
    created_at TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS products (
    product_sku TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price REAL,
    country_of_origin TEXT,
    active INTEGER
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    product_sku TEXT,
    quantity INTEGER,
    order_total REAL,
    order_date TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    order_id TEXT,
    transaction_type TEXT,
    amount REAL,
    transaction_ts TEXT,
    source_system TEXT
);

INSERT OR IGNORE INTO departments (dept_id, dept_name, manager, location, budget) VALUES
('HR', 'People Operations', 'Sophia Chen', 'Singapore', 980000),
('finance', 'Corporate Finance', 'Leila Benali', 'Paris', 1625000),
('support', 'Customer Support', 'Zara Ali', 'Dubai', 870000);

INSERT OR IGNORE INTO employees (id, full_name, age, email, department, salary, join_date, status) VALUES
(1, 'Sophia Chen', 38, 'sophia.chen@corp.example', 'HR', 69000, '2023-09-10', 'Leave'),
(2, 'Zara Ali', 31, 'zara.ali@corp.example', 'Finance', 83000, '2022-07-19', 'ACTIVE'),
(3, 'Late Arrival', 44, 'late.arrival@@corp.example', 'Sales', 59000, '2018-06-30', 'Inactive');

INSERT OR IGNORE INTO customers (customer_id, customer_name, email, country, city, loyalty_tier, created_at, status) VALUES
('C004', 'Orchid Ventures', 'billing_at_orchid.example', 'France', 'Paris', 'silver', '2024-03-09 11:00:00', 'active'),
('C005', 'Desert Peak', NULL, 'AE', 'Dubai', 'bronze', 'invalid-date', 'pending');

INSERT OR IGNORE INTO products (product_sku, product_name, category, unit_price, country_of_origin, active) VALUES
('SKU-005', 'Onsite Gateway', 'networking', 510.00, 'Italy', 1),
('SKU-006', 'Legacy Adapter', 'hardware', 89.99, 'china', 0);

INSERT OR IGNORE INTO orders (order_id, customer_id, product_sku, quantity, order_total, order_date, status) VALUES
('O-1003', 'C005', 'SKU-005', 1, 510.00, '2021-12-11', 'LATE_ARRIVING'),
('O-1006', 'C004', 'SKU-006', 10, 899.90, '2026-13-01', 'FAILED_VALIDATION');

INSERT OR IGNORE INTO transactions (transaction_id, order_id, transaction_type, amount, transaction_ts, source_system) VALUES
('T-9005', 'O-1003', 'SETTLED', 510.00, '2026-05-11T21:00:00Z', 'sqlite'),
('T-9006', 'O-1006', 'REVERSAL', -899.90, 'bad-timestamp', 'sqlite');