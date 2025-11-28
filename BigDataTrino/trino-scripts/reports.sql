-- 1. Витрина продаж по продуктам
INSERT INTO reports_clickhouse.default.top10_products
SELECT
    p.product_id,
    p.name as product_name,
    SUM(fs.total_price) as total_sales,
    SUM(fs.quantity) as total_quantity,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.total_price) DESC) as rank
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.products p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.name
ORDER BY total_sales DESC
LIMIT 10;

INSERT INTO reports_clickhouse.default.revenue_by_category
SELECT
    p.category,
    SUM(fs.total_price) as total_revenue,
    COUNT(DISTINCT p.product_id) as product_count
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.products p ON fs.product_id = p.product_id
GROUP BY p.category;

INSERT INTO reports_clickhouse.default.product_ratings
SELECT
    p.product_id,
    p.name as product_name,
    p.rating as avg_rating,
    p.reviews as total_reviews,
    SUM(fs.quantity) as total_sales
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.products p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.name, p.rating, p.reviews;

-- 2. Витрина продаж по клиентам
INSERT INTO reports_clickhouse.default.top10_customers
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(fs.total_price) as total_spent,
    COUNT(fs.sale_id) as purchase_count,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.total_price) DESC) as rank
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.customers c ON fs.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC
LIMIT 10;

INSERT INTO reports_clickhouse.default.customers_by_country
SELECT
    c.country,
    COUNT(DISTINCT c.customer_id) as customer_count,
    SUM(fs.total_price) as total_spent
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.customers c ON fs.customer_id = c.customer_id
GROUP BY c.country;

INSERT INTO reports_clickhouse.default.avg_check_by_customer
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    AVG(fs.total_price) as avg_check,
    COUNT(fs.sale_id) as purchase_count
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.customers c ON fs.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- 3. Витрина продаж по времени
INSERT INTO reports_clickhouse.default.monthly_trends
SELECT
    year(fs.sale_date) as year,
    month(fs.sale_date) as month,
    SUM(fs.total_price) as total_revenue,
    COUNT(fs.sale_id) as total_orders,
    AVG(fs.total_price) as avg_order_value
FROM etl_clickhouse.default.fact_sales fs
GROUP BY year(fs.sale_date), month(fs.sale_date);

INSERT INTO reports_clickhouse.default.yearly_trends
SELECT
    year(fs.sale_date) as year,
    SUM(fs.total_price) as total_revenue,
    COUNT(fs.sale_id) as total_orders,
    AVG(fs.total_price) as avg_order_value
FROM etl_clickhouse.default.fact_sales fs
GROUP BY year(fs.sale_date);

-- 4. Витрина продаж по магазинам
INSERT INTO reports_clickhouse.default.top5_stores
SELECT
    st.store_id,
    st.name as store_name,
    SUM(fs.total_price) as total_revenue,
    COUNT(fs.sale_id) as order_count,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.total_price) DESC) as rank
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.stores st ON fs.store_id = st.store_id
GROUP BY st.store_id, st.name
ORDER BY total_revenue DESC
LIMIT 5;

INSERT INTO reports_clickhouse.default.sales_by_city_country
SELECT
    st.country,
    st.city,
    SUM(fs.total_price) as total_revenue,
    COUNT(DISTINCT st.store_id) as store_count
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.stores st ON fs.store_id = st.store_id
GROUP BY st.country, st.city;

INSERT INTO reports_clickhouse.default.avg_check_by_store
SELECT
    st.store_id,
    st.name as store_name,
    AVG(fs.total_price) as avg_check,
    COUNT(fs.sale_id) as order_count
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.stores st ON fs.store_id = st.store_id
GROUP BY st.store_id, st.name;

-- 5. Витрина продаж по поставщикам
INSERT INTO reports_clickhouse.default.top5_suppliers
SELECT
    sup.supplier_id,
    sup.name as supplier_name,
    SUM(fs.total_price) as total_revenue,
    COUNT(DISTINCT fs.product_id) as product_count,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.total_price) DESC) as rank
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.suppliers sup ON fs.supplier_id = sup.supplier_id
GROUP BY sup.supplier_id, sup.name
ORDER BY total_revenue DESC
LIMIT 5;

INSERT INTO reports_clickhouse.default.avg_price_by_supplier
SELECT
    sup.supplier_id,
    sup.name as supplier_name,
    AVG(p.price) as avg_product_price,
    COUNT(DISTINCT p.product_id) as product_count
FROM etl_clickhouse.default.products p
         JOIN etl_clickhouse.default.fact_sales fs ON p.product_id = fs.product_id
         JOIN etl_clickhouse.default.suppliers sup ON fs.supplier_id = sup.supplier_id
GROUP BY sup.supplier_id, sup.name;

INSERT INTO reports_clickhouse.default.sales_by_supplier_country
SELECT
    sup.country,
    SUM(fs.total_price) as total_revenue,
    COUNT(DISTINCT sup.supplier_id) as supplier_count
FROM etl_clickhouse.default.fact_sales fs
         JOIN etl_clickhouse.default.suppliers sup ON fs.supplier_id = sup.supplier_id
GROUP BY sup.country;

-- 6. Витрина качества продукции
INSERT INTO reports_clickhouse.default.highest_rated_products
SELECT
    p.product_id,
    p.name as product_name,
    p.rating,
    p.reviews as review_count,
    SUM(fs.quantity) as total_sales
FROM etl_clickhouse.default.products p
         JOIN etl_clickhouse.default.fact_sales fs ON p.product_id = fs.product_id
WHERE p.rating IS NOT NULL
GROUP BY p.product_id, p.name, p.rating, p.reviews
ORDER BY p.rating DESC
LIMIT 20;

INSERT INTO reports_clickhouse.default.lowest_rated_products
SELECT
    p.product_id,
    p.name as product_name,
    p.rating,
    p.reviews as review_count,
    SUM(fs.quantity) as total_sales
FROM etl_clickhouse.default.products p
         JOIN etl_clickhouse.default.fact_sales fs ON p.product_id = fs.product_id
WHERE p.rating IS NOT NULL AND p.rating > 0
GROUP BY p.product_id, p.name, p.rating, p.reviews
ORDER BY p.rating ASC
LIMIT 20;

INSERT INTO reports_clickhouse.default.rating_sales_correlation
SELECT
    to_utf8(
            CASE
                WHEN p.rating >= 4.5 THEN '4.5-5.0'
                WHEN p.rating >= 4.0 THEN '4.0-4.4'
                WHEN p.rating >= 3.5 THEN '3.5-3.9'
                WHEN p.rating >= 3.0 THEN '3.0-3.4'
                ELSE 'Below 3.0'
                END
    ) as rating_range,
    CAST(AVG(p.rating) AS DOUBLE) as avg_rating,
    CAST(SUM(fs.quantity) AS BIGINT) as total_sales,
    CAST(SUM(fs.total_price) AS DOUBLE) as total_revenue,
    CAST(COUNT(DISTINCT p.product_id) AS BIGINT) as product_count
FROM etl_clickhouse.default.products p
         JOIN etl_clickhouse.default.fact_sales fs ON p.product_id = fs.product_id
WHERE p.rating IS NOT NULL
GROUP BY
    CASE
        WHEN p.rating >= 4.5 THEN '4.5-5.0'
        WHEN p.rating >= 4.0 THEN '4.0-4.4'
        WHEN p.rating >= 3.5 THEN '3.5-3.9'
        WHEN p.rating >= 3.0 THEN '3.0-3.4'
        ELSE 'Below 3.0'
        END;

INSERT INTO reports_clickhouse.default.top_reviewed_products
SELECT
    p.product_id,
    p.name as product_name,
    p.reviews as review_count,
    p.rating,
    SUM(fs.quantity) as total_sales
FROM etl_clickhouse.default.products p
         JOIN etl_clickhouse.default.fact_sales fs ON p.product_id = fs.product_id
WHERE p.reviews IS NOT NULL
GROUP BY p.product_id, p.name, p.reviews, p.rating
ORDER BY p.reviews DESC
LIMIT 20;