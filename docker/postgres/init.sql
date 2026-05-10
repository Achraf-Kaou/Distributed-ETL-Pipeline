CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(150) UNIQUE NOT NULL,
    country    VARCHAR(60),
    age        INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    category    VARCHAR(80),
    price       NUMERIC(10, 2),
    stock       INT DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email, country, age) VALUES
    ('Alice Martin',    'alice@example.com',   'France',        29),
    ('Bob Smith',       'bob@example.com',     'USA',           34),
    ('Carlos Diaz',     'carlos@example.com',  'Spain',         41),
    ('Diana Prince',    'diana@example.com',   'UK',            27),
    ('Emre Yilmaz',     'emre@example.com',    'Turkey',        38),
    ('Fatima Nour',     'fatima@example.com',  'Tunisia',       31),
    ('George Huang',    'george@example.com',  'China',         45),
    ('Hana Müller',     'hana@example.com',    'Germany',       26),
    ('Ivan Petrov',     'ivan@example.com',    'Russia',        52),
    ('Julia Santos',    'julia@example.com',   'Brazil',        33);

INSERT INTO products (name, category, price, stock) VALUES
    ('Laptop Pro 15',       'Electronics',   1299.99,  50),
    ('Wireless Mouse',      'Electronics',     25.49, 200),
    ('Standing Desk',       'Furniture',      349.00,  30),
    ('Ergonomic Chair',     'Furniture',      499.00,  15),
    ('USB-C Hub 7-in-1',   'Electronics',     45.00, 120),
    ('Notebook A5',         'Stationery',       4.99, 500),
    ('Mechanical Keyboard', 'Electronics',    115.00,  80),
    ('Monitor 27" 4K',      'Electronics',    549.00,  25),
    ('Desk Lamp LED',       'Furniture',       39.99,  90),
    ('Webcam HD 1080p',     'Electronics',     79.99,  60);