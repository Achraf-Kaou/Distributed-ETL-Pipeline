CREATE TABLE IF NOT EXISTS employees (

    id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(100),
    age INT,
@@ -10,14 +9,76 @@ CREATE TABLE employees (
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS departments (
    dept_id VARCHAR(20) PRIMARY KEY,
    dept_name VARCHAR(100),
    manager VARCHAR(100),
    location VARCHAR(100),
    budget DECIMAL(15,2)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(120),
    email VARCHAR(120),
    country VARCHAR(60),
    city VARCHAR(80),
    loyalty_tier VARCHAR(30),
    created_at DATETIME,
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS products (
    product_sku VARCHAR(30) PRIMARY KEY,
    product_name VARCHAR(120),
    category VARCHAR(80),
    unit_price DECIMAL(12,2),
    country_of_origin VARCHAR(60),
    active BOOLEAN
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20),
    product_sku VARCHAR(30),
    quantity INT,
    order_total DECIMAL(12,2),
    order_date DATE,
    status VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    order_id VARCHAR(30),
    transaction_type VARCHAR(30),
    amount DECIMAL(12,2),
    transaction_ts DATETIME,
    source_system VARCHAR(30)
);

INSERT IGNORE INTO departments (dept_id, dept_name, manager, location, budget) VALUES
('sales', 'Global Sales', 'Amelia Hart', 'New York, USA', 2100000),
('IT', 'Technology & Platforms', 'Noah Singh', 'Toronto', 1850000),
('operations', 'Field Operations', 'Marco Rossi', 'Rome', 1210000),
('finance', 'Corporate Finance', 'Leila Benali', 'Paris', 1625000);

INSERT IGNORE INTO employees (full_name, age, email, department, salary, join_date, status) VALUES
('Noah Singh', 35, 'noah.singh@corp.example', 'IT', 99000, '2020-11-03', 'Active'),
('Marco Rossi', 30, 'marco.rossi@corp.example', 'Operations', 56000, '2024-03-01', 'Active'),
('Avery Stone', NULL, 'avery.stone@corp.example', 'IT', 65000, '2024-10-11', NULL);

INSERT IGNORE INTO customers (customer_id, customer_name, email, country, city, loyalty_tier, created_at, status) VALUES
('C003', 'Blue Ocean Trading', 'operations@blueocean.example', 'United Kingdom', 'london', 'gold', '2024-11-02 08:15:00', 'active'),
('C006', 'Maple Labs', 'sales@maplelabs.example', 'CA / Toronto', 'Toronto, CA', 'gold', '2022-08-11 07:45:00', 'inactive');

INSERT IGNORE INTO products (product_sku, product_name, category, unit_price, country_of_origin, active) VALUES
('SKU-003', 'Field Sensor', 'hardware', 395.00, 'GERMANY', true),
('SKU-004', 'Back Office License', 'software', 249.00, 'Ireland', true);

INSERT IGNORE INTO orders (order_id, customer_id, product_sku, quantity, order_total, order_date, status) VALUES
('O-1002', 'C002', 'SKU-003', 2, 780.00, '2026-04-30', 'PROCESSING'),
('O-1005', 'C006', 'SKU-004', 5, 1245.00, '2026-05-03', 'NEW');

INSERT IGNORE INTO transactions (transaction_id, order_id, transaction_type, amount, transaction_ts, source_system) VALUES
('T-9003', 'O-1002', 'AUTH_CAPTURE', 780.00, '2026-04-30 12:00:00', 'mysql'),
('T-9004', 'O-1005', 'AUTH_CAPTURE', 1245.00, '2026-05-03 09:20:00', 'mysql');