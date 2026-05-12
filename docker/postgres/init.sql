CREATE TABLE IF NOT EXISTS employees (

    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    age INT,
@@ -11,20 +10,84 @@ CREATE TABLE employees (
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS departments (
    dept_id VARCHAR(20) PRIMARY KEY,
    dept_name VARCHAR(100),
    manager VARCHAR(100),
    location VARCHAR(100),
    budget NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(120),
    email VARCHAR(120),
    country VARCHAR(60),
    city VARCHAR(80),
    loyalty_tier VARCHAR(30),
    created_at TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS products (
    product_sku VARCHAR(30) PRIMARY KEY,
    product_name VARCHAR(120),
    category VARCHAR(80),
    unit_price NUMERIC(12,2),
    country_of_origin VARCHAR(60),
    active BOOLEAN
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20),
    product_sku VARCHAR(30),
    quantity INT,
    order_total NUMERIC(12,2),
    order_date DATE,
    status VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    order_id VARCHAR(30),
    transaction_type VARCHAR(30),
    amount NUMERIC(12,2),
    transaction_ts TIMESTAMP,
    source_system VARCHAR(30)
);

INSERT INTO departments (dept_id, dept_name, manager, location, budget) VALUES
('sales', 'Global Sales', 'Amelia Hart', 'New York, USA', 2100000),
('IT', 'Technology & Platforms', 'Noah Singh', 'Toronto', 1850000),
('finance', 'Corporate Finance', 'Leila Benali', 'Paris', 1625000),
('HR', 'People Operations', 'Sophia Chen', 'Singapore', 980000)
ON CONFLICT (dept_id) DO NOTHING;

INSERT INTO employees (full_name, age, email, department, salary, join_date, status) VALUES
('Amelia Hart', 42, 'amelia.hart@northwind.com', 'Sales', 91000, '2025-01-15', 'Active'),
('Noah Singh', 36, 'noah.singh@corp.example', 'Technology', 102000, '2024-12-01', 'Active'),
('Leila Benali', 34, 'leila.benali@corp.example', 'Finance', 79000, '2024-03-05', 'Active'),
('Sophia Chen', 39, 'sophia.chen@corp.example', 'HR', 70500, '2025-02-14', 'Active')
ON CONFLICT (email) DO NOTHING;

INSERT INTO customers (customer_id, customer_name, email, country, city, loyalty_tier, created_at, status) VALUES
('C001', 'Northwind Retail', 'contact@northwind.example', 'USA', 'New York', 'platinum', '2023-01-15 09:00:00', 'active'),
('C002', 'Blue Ocean Trading', 'operations@blueocean.example', 'UK', 'London', 'gold', '2023-06-21 12:30:00', 'active')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO products (product_sku, product_name, category, unit_price, country_of_origin, active) VALUES
('SKU-001', 'Enterprise Router', 'networking', 300.13, 'USA', true),
('SKU-002', 'Analytics Seat', 'software', 150.00, 'Ireland', true),
('SKU-003', 'Field Sensor', 'hardware', 390.00, 'Germany', true)
ON CONFLICT (product_sku) DO NOTHING;

INSERT INTO orders (order_id, customer_id, product_sku, quantity, order_total, order_date, status) VALUES
('O-1001', 'C001', 'SKU-001', 4, 1200.50, '2026-04-30', 'SHIPPED'),
('O-1004', 'C002', 'SKU-003', 1, 390.00, '2026-05-02', 'PROCESSING')
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO transactions (transaction_id, order_id, transaction_type, amount, transaction_ts, source_system) VALUES
('T-9001', 'O-1001', 'AUTH_CAPTURE', 1200.50, '2026-04-30 10:11:12', 'postgres'),
('T-9002', 'O-1004', 'AUTH_ONLY', 390.00, '2026-05-02 13:00:00', 'postgres')
ON CONFLICT (transaction_id) DO NOTHING;