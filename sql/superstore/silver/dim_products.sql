CREATE TABLE IF NOT EXISTS silver_dim_products (
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    category VARCHAR(255),
    subcategory VARCHAR(255),
    CONSTRAINT pk_silver_dim_products PRIMARY KEY (product_id, product_name)
);

INSERT INTO silver_dim_products (product_id, product_name, category, subcategory)
SELECT DISTINCT 
    "Product ID" AS product_id,
    "Product Name" AS product_name,
    "Category" AS category,
    "Sub-Category" AS subcategory
FROM SuperStore.bronze_transaction b
WHERE NOT EXISTS (
    SELECT 1
    FROM silver_dim_products s
    WHERE b."Product ID" = s.product_id
      AND b."Product Name" = s.product_name
);
