-- HiveQL: Create and seed sample Hive tables for DQ Engine demo
-- Run via beeline after HiveServer2 is healthy:
--   beeline -u 'jdbc:hive2://localhost:10000/' -f db/hive_datasource_schema.hql

CREATE DATABASE IF NOT EXISTS dq_demo;
USE dq_demo;

DROP TABLE IF EXISTS dq_demo.transactions;
CREATE TABLE dq_demo.transactions (
    txn_id      BIGINT,
    customer_id INT,
    amount      DECIMAL(12,2),
    currency    STRING,
    txn_type    STRING,
    txn_date    STRING
)
STORED AS PARQUET;

-- Seed data: mix of valid rows, nulls, invalid currencies, negative amounts
INSERT INTO dq_demo.transactions VALUES
(1001, 101, 250.00,  'USD', 'purchase',  '2024-01-03'),
(1002, 102, 89.99,   'USD', 'purchase',  '2024-01-03'),
(1003, 103, NULL,    'GBP', 'refund',    '2024-01-03'),   -- null amount
(1004, 104, 1500.00, 'EUR', 'purchase',  '2024-01-03'),
(1005, 105, -50.00,  'USD', 'purchase',  '2024-01-03'),   -- negative amount
(1006, 106, 320.00,  'XYZ', 'purchase',  '2024-01-03'),   -- invalid currency
(1007, 107, 75.50,   'GBP', 'purchase',  '2024-01-03'),
(1008, NULL,450.00,  'USD', 'purchase',  '2024-01-03'),   -- null customer_id
(1009, 109, 200.00,  'EUR', 'UNKNOWN',   '2024-01-03'),   -- invalid txn_type
(1010, 110, 95.00,   'USD', 'purchase',  '2024-01-03');
