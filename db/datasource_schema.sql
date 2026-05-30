-- Sample data source schema (applied to the mariadb-datasource service, database: dq_datasource)
CREATE TABLE IF NOT EXISTS orders (
    order_id    INT          NOT NULL AUTO_INCREMENT,
    customer_id INT          NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    status      VARCHAR(20)  NOT NULL,
    order_date  DATE         NOT NULL,
    PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS products (
    product_id  INT          NOT NULL AUTO_INCREMENT,
    name        VARCHAR(256) NOT NULL,
    price       DECIMAL(10,2) NULL,        -- intentionally nullable so DQ checks can detect missing prices
    category    VARCHAR(64)  NULL,
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
