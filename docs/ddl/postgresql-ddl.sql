-- Drop tables if they already exist (for cleanup)
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
DROP TYPE IF EXISTS order_status;

-- Create ENUM type for order status
CREATE TYPE order_status AS ENUM ('PENDING', 'COMPLETED', 'CANCELED');

-- Create "users" table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create "orders" table (related to "users")
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
    status order_status NOT NULL,  -- Uses ENUM type
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Insert dummy users
INSERT INTO users (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com'),
    ('David Williams', 'david@example.com'),
    ('Emma Thompson', 'emma@example.com'),
    ('Frank Miller', 'frank@example.com'),
    ('Grace Wilson', 'grace@example.com'),
    ('Henry Moore', 'henry@example.com'),
    ('Ivy Davis', 'ivy@example.com'),
    ('Jack White', 'jack@example.com');

-- Insert dummy orders
INSERT INTO orders (user_id, amount, status) VALUES
    -- Alice (3 orders)
    (1, 50.75, 'PENDING'), (1, 99.99, 'COMPLETED'), (1, 45.50, 'COMPLETED'),
    -- Bob (4 orders)
    (2, 120.00, 'COMPLETED'), (2, 65.75, 'PENDING'), (2, 30.99, 'CANCELED'), (2, 89.25, 'COMPLETED'),
    -- Charlie (5 orders)
    (3, 150.50, 'COMPLETED'), (3, 42.30, 'PENDING'), (3, 78.90, 'PENDING'), (3, 210.00, 'COMPLETED'), (3, 55.60, 'CANCELED'),
    -- David (6 orders)
    (4, 125.00, 'COMPLETED'), (4, 95.75, 'PENDING'), (4, 40.00, 'COMPLETED'), (4, 20.00, 'CANCELED'), (4, 135.20, 'COMPLETED'), (4, 99.99, 'COMPLETED'),
    -- Emma (4 orders)
    (5, 75.00, 'PENDING'), (5, 44.50, 'COMPLETED'), (5, 88.75, 'PENDING'), (5, 200.00, 'COMPLETED'),
    -- Frank (3 orders)
    (6, 50.25, 'PENDING'), (6, 110.50, 'COMPLETED'), (6, 33.33, 'CANCELED'),
    -- Grace (7 orders)
    (7, 99.00, 'COMPLETED'), (7, 67.45, 'PENDING'), (7, 55.75, 'PENDING'), (7, 123.45, 'COMPLETED'), (7, 90.00, 'CANCELED'),
    -- Henry (5 orders)
    (8, 160.75, 'COMPLETED'), (8, 72.50, 'PENDING'), (8, 105.99, 'COMPLETED'), (8, 38.25, 'CANCELED'), (8, 129.75, 'COMPLETED'),
    -- Ivy (4 orders)
    (9, 80.50, 'PENDING'), (9, 142.00, 'COMPLETED'), (9, 95.99, 'COMPLETED'), (9, 58.50, 'PENDING'),
    -- Jack (6 orders)
    (10, 65.30, 'PENDING'), (10, 99.99, 'COMPLETED'), (10, 120.00, 'COMPLETED'), (10, 55.75, 'PENDING'), (10, 44.00, 'CANCELED'), (10, 199.99, 'COMPLETED');
