-- Seed DQ configuration for demo-v1
-- Covers all 3 source types, all 3 check_types, all 3 evaluation_modes, 3 app_codes

DELETE FROM result_store  WHERE config_version = 'demo-v1';
DELETE FROM config_store  WHERE config_version = 'demo-v1';
DELETE FROM config_versions WHERE config_version = 'demo-v1';
DELETE FROM source_catalog;

INSERT INTO config_versions (config_version, config_version_date) VALUES
('demo-v1', '2024-01-01 09:00:00.000');

-- Source catalog: customers (Parquet), orders/products (MariaDB), transactions (Hive)
INSERT INTO source_catalog
  (table_name, source_type, parquet_location, partition_column, connection_ref, physical_table_name, filter_column)
VALUES
  ('customers',    'parquet',       '/opt/dq/data/parquet/customers', 'date',  NULL,              NULL,           NULL),
  ('orders',       'mariadb_table', NULL, NULL,                                'main-datasource', 'orders',       NULL),
  ('products',     'mariadb_table', NULL, NULL,                                'main-datasource', 'products',     NULL),
  ('transactions', 'hive_table',    NULL, NULL,                                'main-hive',       'dq_demo.transactions', NULL);

-- ── CRM app checks (customers Parquet source) ──────────────────────────────────

INSERT INTO config_store
  (config_version, check_id, table_name, column_name, data_type, check_type,
   business_glossary, business_rule_filter, deviation_tolerance, check_sql, evaluation_mode, app_code)
VALUES
-- Completeness: no null names
('demo-v1','CRM-COMPL-NAME','customers','name','STRING','completeness',
 'Customer name must not be null',NULL,NULL,
 'SELECT customer_id FROM customers WHERE name IS NULL',
 'violation','CRM'),

-- Completeness: no null emails for active customers only (conditional null check)
('demo-v1','CRM-COMPL-EMAIL-ACTIVE','customers','email','STRING','completeness',
 'Active customers must have an email address','status = ''active''',NULL,
 'SELECT customer_id FROM customers WHERE email IS NULL',
 'violation','CRM'),

-- Validity categorical: status must be in allowed set
('demo-v1','CRM-VAL-STATUS','customers','status','STRING','validity',
 'Status must be active, inactive or pending',NULL,NULL,
 'SELECT customer_id FROM customers WHERE status NOT IN (''active'',''inactive'',''pending'')',
 'violation','CRM'),

-- Validity continuous: age must be between 0 and 120
('demo-v1','CRM-VAL-AGE','customers','age','INT','validity',
 'Age must be between 0 and 120',NULL,NULL,
 'SELECT customer_id FROM customers WHERE age < 0 OR age > 120',
 'violation','CRM'),

-- Consistency delta: row count should not drop by more than 50 rows
('demo-v1','CRM-CONS-COUNT','customers','customer_id','INT','consistency',
 'Daily customer count should be stable (within 50 rows)',NULL,50,
 'SELECT COUNT(*) AS cnt FROM customers',
 'consistency_delta','CRM'),

-- Consistency MOM: row count should not change more than 15% month-over-month
('demo-v1','CRM-MOM-COUNT','customers','customer_id','INT','consistency',
 'Monthly customer count should not vary more than 15%',NULL,15,
 'SELECT COUNT(*) AS cnt FROM customers',
 'consistency_mom','CRM'),

-- ── ORDER_MGMT app checks (orders MariaDB source) ──────────────────────────────

-- Completeness: no null customer_id on orders
('demo-v1','ORD-COMPL-CUST','orders','customer_id','INT','completeness',
 'Orders must have a customer_id',NULL,NULL,
 'SELECT order_id FROM orders WHERE customer_id IS NULL',
 'violation','ORDER_MGMT'),

-- Validity continuous: no negative amounts
('demo-v1','ORD-VAL-AMOUNT','orders','amount','DECIMAL','validity',
 'Order amount must not be negative',NULL,NULL,
 'SELECT order_id FROM orders WHERE amount < 0',
 'violation','ORDER_MGMT'),

-- Validity categorical: status must be in allowed set
('demo-v1','ORD-VAL-STATUS','orders','status','STRING','validity',
 'Order status must be a recognised value',NULL,NULL,
 'SELECT order_id FROM orders WHERE status NOT IN (''completed'',''pending'',''shipped'',''cancelled'',''refunded'')',
 'violation','ORDER_MGMT'),

-- Consistency delta: completed order count should not drop more than 5
('demo-v1','ORD-CONS-COMPLETED','orders','status','INT','consistency',
 'Completed order count should be stable',NULL,5,
 'SELECT COUNT(*) AS cnt FROM orders WHERE status = ''completed''',
 'consistency_delta','ORDER_MGMT'),

-- ── INVENTORY app checks (products MariaDB source) ────────────────────────────

-- Completeness: no null prices
('demo-v1','INV-COMPL-PRICE','products','price','DECIMAL','completeness',
 'Product price must not be null',NULL,NULL,
 'SELECT product_id FROM products WHERE price IS NULL',
 'violation','INVENTORY'),

-- Validity continuous: price must be positive
('demo-v1','INV-VAL-PRICE','products','price','DECIMAL','validity',
 'Product price must be greater than zero',NULL,NULL,
 'SELECT product_id FROM products WHERE price IS NOT NULL AND price <= 0',
 'violation','INVENTORY'),

-- ── FINANCE app checks (transactions Hive source) ────────────────────────────

-- Completeness: no null amounts in transactions
('demo-v1','FIN-COMPL-AMOUNT','transactions','amount','DECIMAL','completeness',
 'Transaction amount must not be null',NULL,NULL,
 'SELECT txn_id FROM dq_demo.transactions WHERE amount IS NULL',
 'violation','FINANCE'),

-- Validity categorical: currency must be ISO-4217 (USD, GBP, EUR only for demo)
('demo-v1','FIN-VAL-CURRENCY','transactions','currency','STRING','validity',
 'Currency must be USD, GBP or EUR',NULL,NULL,
 'SELECT txn_id FROM dq_demo.transactions WHERE currency NOT IN (''USD'',''GBP'',''EUR'')',
 'violation','FINANCE'),

-- Validity continuous: transaction amount must be non-negative (refunds handled by txn_type)
('demo-v1','FIN-VAL-AMOUNT','transactions','amount','DECIMAL','validity',
 'Transaction amount must be non-negative',NULL,NULL,
 'SELECT txn_id FROM dq_demo.transactions WHERE amount IS NOT NULL AND amount < 0',
 'violation','FINANCE'),

-- Consistency delta: total transaction count should not vary by more than 2
('demo-v1','FIN-CONS-TXN-COUNT','transactions','txn_id','INT','consistency',
 'Daily transaction count should be stable',NULL,2,
 'SELECT COUNT(*) AS cnt FROM dq_demo.transactions',
 'consistency_delta','FINANCE');
