-- Seed data for the mariadb-datasource service (database: dq_datasource)
-- Used for testing mariadb_table DQ checks

TRUNCATE TABLE orders;
TRUNCATE TABLE products;

-- products
INSERT INTO products (product_id, name, price, category) VALUES
(1,  'Widget A',         9.99,   'widgets'),
(2,  'Widget B',        14.99,   'widgets'),
(3,  'Gadget Pro',     149.99,  'gadgets'),
(4,  'Gadget Lite',    49.99,   'gadgets'),
(5,  'Thingamajig',    29.99,   'misc'),
(6,  'Doohickey',       NULL,   'misc'),  -- intentional: NULL price for DQ check
(7,  'Super Widget',   19.99,   'widgets'),
(8,  'Budget Gadget',  24.99,   'gadgets');

-- orders — majority valid, a few intentional DQ issues
INSERT INTO orders (order_id, customer_id, amount, status, order_date) VALUES
(1,  101,  29.99,  'completed',  '2024-01-01'),
(2,  102,  149.99, 'completed',  '2024-01-01'),
(3,  103,  -5.00,  'refunded',   '2024-01-01'),   -- negative amount (DQ violation)
(4,  104,  9.99,   'completed',  '2024-01-02'),
(5,  105,  49.99,  'pending',    '2024-01-02'),
(6,  106,  29.99,  'completed',  '2024-01-02'),
(7,  107,  14.99,  'completed',  '2024-01-03'),
(8,  108,  149.99, 'completed',  '2024-01-03'),
(9,  109,  -12.50, 'refunded',   '2024-01-03'),   -- negative amount (DQ violation)
(10, 110,  9.99,   'completed',  '2024-01-03'),
(11, 111,  24.99,  'shipped',    '2024-01-04'),
(12, 112,  19.99,  'completed',  '2024-01-04'),
(13, 113,  0.00,   'cancelled',  '2024-01-04'),   -- zero amount (borderline)
(14, 114,  29.99,  'completed',  '2024-01-05'),
(15, 115,  49.99,  'completed',  '2024-01-05');
