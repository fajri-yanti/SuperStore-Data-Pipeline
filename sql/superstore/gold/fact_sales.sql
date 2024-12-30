CREATE TABLE IF NOT EXISTS gold_fact_sales (
    "row_id" SERIAL,
    "order_id" VARCHAR(100),
    "product_id" VARCHAR(100),
    "product_name" VARCHAR(255),
    "customer_id" VARCHAR(100),
    "order_date" DATE,
    "sales" DECIMAL(10, 2),
    "profit" DECIMAL(10, 2),
    "discount" DECIMAL(4, 2),
    "quantity" INT,
    PRIMARY KEY ("order_id", "row_id"),
    FOREIGN KEY ("product_id", "product_name") REFERENCES silver_dim_products ("product_id", "product_name")
    FOREIGN KEY ("customer_id") REFERENCES silver_dim_customers ("customer_id")
);

INSERT INTO gold_fact_sales (
    "order_id",
    "product_id",
    "product_name",
    "customer_id",
    "order_date",
    "sales",
    "profit",
    "discount",
    "quantity"
)
SELECT
    "order_id",
    "product_id",
    "product_name",
    "customer_id",
    "order_date",
    "sales",
    "profit",
    "discount",
    "quantity"
FROM silver_dim_orders;
