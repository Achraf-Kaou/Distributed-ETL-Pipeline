-- MySQL Init
CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(100),
    age INT,
    email VARCHAR(100) UNIQUE,
    department VARCHAR(50),
    salary DECIMAL(12,2),
    join_date DATE,
    status VARCHAR(20)
);

CREATE TABLE departments (
    dept_id VARCHAR(10) PRIMARY KEY,
    dept_name VARCHAR(100),
    manager VARCHAR(100),
    location VARCHAR(100),
    budget DECIMAL(15,2)
);

INSERT INTO departments VALUES 
('IT', 'Information Technology', 'Alice Dupont', 'Paris', 1200000),
('Finance', 'Finance Department', 'Paul Dubois', 'Marseille', 890000);