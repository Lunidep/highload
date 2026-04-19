CREATE DATABASE IF NOT EXISTS default;

-- 1. Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS default.top10_products
(
    product_id     UInt32,
    product_name   String,
    total_sales    Decimal64(2),
    total_quantity UInt32,
    rank           UInt8
) ENGINE = MergeTree()
ORDER BY (rank, product_id);

CREATE TABLE IF NOT EXISTS default.revenue_by_category
(
    category      String,
    total_revenue Decimal64(2),
    product_count UInt32
) ENGINE = MergeTree()
ORDER BY category;

CREATE TABLE IF NOT EXISTS default.product_ratings
(
    product_id    UInt32,
    product_name  String,
    avg_rating    Decimal32(1),
    total_reviews UInt32,
    total_sales   UInt32
) ENGINE = MergeTree()
ORDER BY product_id;

-- 2. Витрина продаж по клиентам
CREATE TABLE IF NOT EXISTS default.top10_customers
(
    customer_id    UInt32,
    first_name     String,
    last_name      String,
    total_spent    Decimal64(2),
    purchase_count UInt32,
    rank           UInt8
) ENGINE = MergeTree()
ORDER BY (rank, customer_id);

CREATE TABLE IF NOT EXISTS default.customers_by_country
(
    country        String,
    customer_count UInt32,
    total_spent    Decimal64(2)
) ENGINE = MergeTree()
ORDER BY country;

CREATE TABLE IF NOT EXISTS default.avg_check_by_customer
(
    customer_id    UInt32,
    first_name     String,
    last_name      String,
    avg_check      Decimal64(2),
    purchase_count UInt32
) ENGINE = MergeTree()
ORDER BY customer_id;

-- 3. Витрина продаж по времени
CREATE TABLE IF NOT EXISTS default.monthly_trends
(
    year            UInt16,
    month           UInt8,
    total_revenue   Decimal64(2),
    total_orders    UInt32,
    avg_order_value Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS default.yearly_trends
(
    year            UInt16,
    total_revenue   Decimal64(2),
    total_orders    UInt32,
    avg_order_value Decimal64(2)
) ENGINE = MergeTree()
ORDER BY year;

-- 4. Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS default.top5_stores
(
    store_id      UInt32,
    store_name    String,
    total_revenue Decimal64(2),
    order_count   UInt32,
    rank          UInt8
) ENGINE = MergeTree()
ORDER BY (rank, store_id);

CREATE TABLE IF NOT EXISTS default.sales_by_city_country
(
    country       String,
    city          String,
    total_revenue Decimal64(2),
    store_count   UInt32
) ENGINE = MergeTree()
ORDER BY (country, city);

CREATE TABLE IF NOT EXISTS default.avg_check_by_store
(
    store_id    UInt32,
    store_name  String,
    avg_check   Decimal64(2),
    order_count UInt32
) ENGINE = MergeTree()
ORDER BY store_id;

-- 5. Витрина продаж по поставщикам
CREATE TABLE IF NOT EXISTS default.top5_suppliers
(
    supplier_id   UInt32,
    supplier_name String,
    total_revenue Decimal64(2),
    product_count UInt32,
    rank          UInt8
) ENGINE = MergeTree()
ORDER BY (rank, supplier_id);

CREATE TABLE IF NOT EXISTS default.avg_price_by_supplier
(
    supplier_id       UInt32,
    supplier_name     String,
    avg_product_price Decimal64(2),
    product_count     UInt32
) ENGINE = MergeTree()
ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS default.sales_by_supplier_country
(
    country        String,
    total_revenue  Decimal64(2),
    supplier_count UInt32
) ENGINE = MergeTree()
ORDER BY country;

-- 6. Витрина качества продукции
CREATE TABLE IF NOT EXISTS default.highest_rated_products
(
    product_id   UInt32,
    product_name String,
    rating       Decimal32(1),
    review_count UInt32,
    total_sales  UInt32
) ENGINE = MergeTree()
ORDER BY (rating, product_id);

CREATE TABLE IF NOT EXISTS default.lowest_rated_products
(
    product_id   UInt32,
    product_name String,
    rating       Decimal32(1),
    review_count UInt32,
    total_sales  UInt32
) ENGINE = MergeTree()
ORDER BY (rating, product_id);

CREATE TABLE IF NOT EXISTS default.rating_sales_correlation
(
    rating_range  String,
    avg_rating    Decimal32(1),
    total_sales   UInt32,
    total_revenue Decimal64(2),
    product_count UInt32
) ENGINE = MergeTree()
ORDER BY rating_range;

CREATE TABLE IF NOT EXISTS default.top_reviewed_products
(
    product_id   UInt32,
    product_name String,
    review_count UInt32,
    rating       Decimal32(1),
    total_sales  UInt32
) ENGINE = MergeTree()
ORDER BY (review_count, product_id);