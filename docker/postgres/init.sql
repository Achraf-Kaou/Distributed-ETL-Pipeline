-- PostgreSQL Init
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    age INT,
    email VARCHAR(100) UNIQUE,
    department VARCHAR(50),
    salary NUMERIC(12,2),
    join_date DATE,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE departments (
    dept_id VARCHAR(10) PRIMARY KEY,
    dept_name VARCHAR(100),
    manager VARCHAR(100),
    location VARCHAR(100),
    budget NUMERIC(15,2)
);

-- Insert sample data
INSERT INTO departments VALUES 
('IT', 'Information Technology', 'Alice Dupont', 'Paris', 1200000),
('HR', 'Human Resources', 'Bob Martin', 'Lyon', 450000);

INSERT INTO employees (full_name, age, email, department, salary, join_date, status)
VALUES 
('Alice Dupont', 28, 'alice.dupont@company.fr', 'IT', 45000, '2023-05-15', 'Active'),
('Bob Martin', 34, 'bob.martin@company.fr', 'HR', 52000, '2022-11-20', 'Active');