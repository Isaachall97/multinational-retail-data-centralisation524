SELECT
    country_code, COUNT(country_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY
    country_code
ORDER BY 
    COUNT(country_code) DESC;

SELECT 
    locality, COUNT(locality)
FROM 
    dim_store_details
GROUP BY
    locality
HAVING
    COUNT(locality)>=10
ORDER BY
    COUNT(locality) DESC;

SELECT DISTINCT
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales, dim_date_times.month
FROM
    orders_table
INNER JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY 
    dim_date_times.month
ORDER BY
    total_sales DESC LIMIT 6;

ALTER TABLE dim_store_details
    ADD COLUMN web_or_offline VARCHAR(7);

UPDATE dim_store_details
SET
    web_or_offline = 'web'
WHERE
    dim_store_details.store_code LIKE 'WEB%';

UPDATE dim_store_details
SET
    web_or_offline = 'offline'
WHERE
    dim_store_details.store_code NOT LIKE 'WEB%';

SELECT
   COUNT(orders_table.product_quantity)AS number_of_sales, SUM(orders_table.product_quantity) AS product_quantity_count, dim_store_details.web_or_offline
FROM 
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    dim_store_details.web_or_offline
ORDER BY
    product_quantity_count;

SELECT
    dim_store_details.store_type,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales,
    SUM(dim_products.product_price * orders_table.product_quantity) / total.total_sales_overall * 100 AS percentage_of_total_sales
FROM 
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
INNER JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code,
    (
        SELECT SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales_overall
        FROM orders_table
        INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
    ) AS total
GROUP BY
    dim_store_details.store_type, total.total_sales_overall
ORDER BY
    total_sales DESC;

SELECT
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales,
    dim_date_times.year, 
    dim_date_times.month
FROM
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    dim_date_times.year, 
    dim_date_times.month
ORDER BY
    total_sales DESC LIMIT 10;

SELECT 
    SUM(dim_store_details.staff_numbers) AS total_staff,
    dim_store_details.country_code
FROM
    dim_store_details
GROUP BY
    dim_store_details.country_code
ORDER BY
    total_staff DESC;

SELECT
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales,
    dim_store_details.store_type,
    dim_store_details.country_code
FROM
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    dim_store_details.store_type,
    dim_store_details.country_code
HAVING 
    dim_store_details.country_code = 'DE'
ORDER BY
    total_sales ASC;

WITH EventTime AS (
    SELECT
        dim_date_times.year,
        TO_TIMESTAMP(
            CAST(dim_date_times.year AS VARCHAR) || '-' ||
            LPAD(CAST(dim_date_times.month AS VARCHAR), 2, '0') || '-' ||
            LPAD(CAST(dim_date_times.day AS VARCHAR), 2, '0') || ' ' ||
            timestamp,
            'YYYY-MM-DD HH24:MI:SS.MS'
        ) AS sale_time
    FROM
        dim_date_times
    GROUP BY
        dim_date_times.year, 
        dim_date_times.month, 
        dim_date_times.day, 
        timestamp
    ORDER BY
        dim_date_times.year, 
        dim_date_times.month, 
        dim_date_times.day,
        timestamp
), 
NextTime AS (
    SELECT
        year,
        sale_time,
        LEAD(sale_time) OVER(
            PARTITION BY year
            ORDER BY sale_time
        ) AS next_sale_time
    FROM
        EventTime
)
SELECT 
    year, 
    AVG(next_sale_time - sale_time) AS avg_time_difference
FROM 
    NextTime
WHERE 
    next_sale_time IS NOT NULL
GROUP BY 
    year
ORDER BY
    avg_time_difference DESC LIMIT 5;

