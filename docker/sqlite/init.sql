CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    country    TEXT,
    age        INTEGER,
    created_at TEXT DEFAULT (datetime('now'))   
);

CREATE TABLE IF NOT EXISTS products (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    category    TEXT,
    price       REAL,
    stock       INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now'))
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