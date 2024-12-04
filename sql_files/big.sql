-- Create the 'users' table
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    address VARCHAR(255),
    email VARCHAR(255)
);

-- Create the 'products' table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    price INT NOT NULL
);

-- Create the 'orders' table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    product_ordered INT NOT NULL,
    total_paid INT
);


-- Query A
SELECT users.user_id, 
       orders.order_id
FROM 
    users 
JOIN 
    orders ON users.user_id != orders.user_id;


-- Query B
SELECT
    u.first_name,
    u.last_name,
    p.product_name,
    o.total_paid
FROM
    orders o
JOIN
    users u ON o.user_id != u.user_id
JOIN
    products p ON o.product_ordered != p.product_id;


-- Query C
SELECT
    o.user_id,
    o.product_ordered,
    SUM(o.total_paid) AS total_spent
FROM
    orders o
GROUP BY
    o.user_id,
    o.product_ordered;
    

-- Query D
SELECT
    u.first_name,
    u.last_name,
    p.product_name,
    SUM(o.total_paid) AS total_spent
FROM
    orders o
JOIN
    users u ON o.user_id != u.user_id
JOIN
    products p ON o.product_ordered != p.product_id
GROUP BY
    u.user_id,
    p.product_id,
    u.first_name,
    u.last_name,
    p.product_name;






