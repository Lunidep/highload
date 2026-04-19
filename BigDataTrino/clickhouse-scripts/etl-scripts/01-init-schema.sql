CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.customers
(
    customer_id UInt32,
    first_name  String,
    last_name   String,
    age         UInt8,
    email       String,
    country     String,
    postal_code Nullable(String),
    pet_type    String,
    pet_name    String,
    pet_breed   String
) ENGINE = MergeTree()
ORDER BY customer_id
PRIMARY KEY customer_id;

CREATE TABLE IF NOT EXISTS default.sellers
(
    seller_id   UInt32,
    first_name  String,
    last_name   String,
    email       String,
    country     String,
    postal_code Nullable(String)
) ENGINE = MergeTree()
ORDER BY seller_id
PRIMARY KEY seller_id;

CREATE TABLE IF NOT EXISTS default.products
(
    product_id   UInt32,
    name         String,
    category     String,
    price        Decimal64(2),
    weight       Decimal64(2),
    color        String,
    size         String,
    brand        String,
    material     String,
    description  String,
    rating       Decimal32(1),
    reviews      UInt32,
    release_date Date,
    expiry_date  Date
) ENGINE = MergeTree()
ORDER BY product_id
PRIMARY KEY product_id;

CREATE TABLE IF NOT EXISTS default.stores
(
    store_id UInt32,
    name     String,
    location String,
    city     String,
    state    Nullable(String),
    country  String,
    phone    String,
    email    String
) ENGINE = MergeTree()
ORDER BY store_id
PRIMARY KEY store_id;

CREATE TABLE IF NOT EXISTS default.suppliers
(
    supplier_id UInt32,
    name        String,
    contact     String,
    email       String,
    phone       String,
    address     String,
    city        String,
    country     String
) ENGINE = MergeTree()
ORDER BY supplier_id
PRIMARY KEY supplier_id;

CREATE TABLE IF NOT EXISTS default.fact_sales
(
    sale_id     UInt32,
    customer_id UInt32,
    seller_id   UInt32,
    product_id  UInt32,
    store_id    UInt32,
    supplier_id UInt32,
    sale_date   Date,
    quantity    UInt32,
    total_price Decimal64(2)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, sale_id)
PRIMARY KEY (sale_date, sale_id);