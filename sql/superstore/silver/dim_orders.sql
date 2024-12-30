CREATE TABLE IF NOT EXISTS silver_dim_orders (
    row_id INT,
    order_id VARCHAR(255),
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(255),
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    sales DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    discount DECIMAL(4, 2),
    quantity INT,
    PRIMARY KEY (row_id, order_id),
    FOREIGN KEY (product_id, product_name) REFERENCES silver_dim_products(product_id, product_name)
);

INSERT INTO silver_dim_orders (
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    product_id,
    product_name,
    sales,
    profit,
    discount,
    quantity
)

SELECT 
    "Row ID" AS row_id,
    "Order ID" AS order_id,
    "Order Date" AS order_date,
    "Ship Date" AS ship_date,
    "Ship Mode" AS ship_mode,
    "Customer ID" AS customer_id,
    "Product ID" AS product_id,
    "Product Name" AS product_name,
    "Sales" AS sales,
    "Profit" AS profit,
    "Discount" AS discount,
    "Quantity" AS quantity
FROM SuperStore.bronze_transaction b 
WHERE NOT EXISTS (
    SELECT 1
    FROM silver_dim_orders o 
    WHERE o.row_id = b."Row ID"
        AND o.order_id = b."Order ID"
);
