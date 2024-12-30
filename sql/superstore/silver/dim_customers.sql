-- CREATE TYPE segment_type AS ENUM ('Consumer', 'Corporate', 'Home Office');

CREATE TABLE IF NOT EXISTS silver_dim_customers (
    customer_id CHAR(100),
    customer_name CHAR(100),
    segment CHAR(100),
    country CHAR(100),
    state CHAR(100),
    region CHAR(100),
    city CHAR(100),
    postal_code CHAR(10)
);
INSERT INTO silver_dim_customers (customer_id, customer_name, segment, country, state, region, city, postal_code)
SELECT DISTINCT 
    b."Customer ID",           -- Kolom dengan spasi dan kapital harus diapit dengan tanda kutip ganda
    b."Customer Name",         -- Kolom dengan spasi dan kapital harus diapit dengan tanda kutip ganda
    b."Segment",               -- Kolom dengan kapital diapit dengan tanda kutip ganda
    b."Country",               -- Kolom dengan kapital diapit dengan tanda kutip ganda
    b."State",                 -- Kolom dengan kapital diapit dengan tanda kutip ganda
    b."Region",                -- Kolom dengan kapital diapit dengan tanda kutip ganda
    b."City",                  -- Kolom dengan kapital diapit dengan tanda kutip ganda
    b."Postal Code"            -- Kolom dengan spasi dan kapital harus diapit dengan tanda kutip ganda
FROM SuperStore.bronze_transaction b
WHERE NOT EXISTS (
    SELECT 1
    FROM silver_dim_customers c
    WHERE c.customer_id = b."Customer ID"
);
