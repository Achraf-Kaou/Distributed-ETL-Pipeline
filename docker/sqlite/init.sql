-- SQLite Init
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    full_name TEXT,
    age INTEGER,
    email TEXT UNIQUE,
    department TEXT,
    salary REAL,
    join_date TEXT,
    status TEXT
);

CREATE TABLE departments (
    dept_id TEXT PRIMARY KEY,
    dept_name TEXT,
    manager TEXT,
    location TEXT,
    budget REAL
);

INSERT INTO employees (full_name, age, email, department, salary, join_date, status)
VALUES 
('Sophie Bernard', 29, 'sophie.b@company.fr', 'Marketing', 49000, '2024-02-28', 'Active');