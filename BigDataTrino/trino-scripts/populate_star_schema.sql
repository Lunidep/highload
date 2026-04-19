INSERT INTO etl_clickhouse.default.customers (customer_id,
                                              first_name,
                                              last_name,
                                              age,
                                              email,
                                              country,
                                              postal_code,
                                              pet_type,
                                              pet_name,
                                              pet_breed)
WITH combined_customers AS (SELECT to_utf8(customer_first_name)  as customer_first_name,
                                   to_utf8(customer_last_name)   as customer_last_name,
                                   customer_age,
                                   to_utf8(customer_email)       as customer_email,
                                   to_utf8(customer_country)     as customer_country,
                                   to_utf8(customer_postal_code) as customer_postal_code,
                                   to_utf8(customer_pet_type)    as customer_pet_type,
                                   to_utf8(customer_pet_name)    as customer_pet_name,
                                   to_utf8(customer_pet_breed)   as customer_pet_breed
                            FROM postgres.public.main_data
                            WHERE customer_first_name IS NOT NULL
                              AND customer_last_name IS NOT NULL

                            UNION ALL

                            SELECT customer_first_name,
                                   customer_last_name,
                                   customer_age,
                                   customer_email,
                                   customer_country,
                                   customer_postal_code,
                                   customer_pet_type,
                                   customer_pet_name,
                                   customer_pet_breed
                            FROM source_clickhouse.default.main_data
                            WHERE customer_first_name IS NOT NULL
                              AND customer_last_name IS NOT NULL),
     unique_customers AS (SELECT customer_first_name,
                                 customer_last_name,
                                 customer_age,
                                 customer_email,
                                 customer_country,
                                 customer_postal_code,
                                 customer_pet_type,
                                 customer_pet_name,
                                 customer_pet_breed
                          FROM combined_customers
                          GROUP BY customer_first_name,
                                   customer_last_name,
                                   customer_age,
                                   customer_email,
                                   customer_country,
                                   customer_postal_code,
                                   customer_pet_type,
                                   customer_pet_name,
                                   customer_pet_breed)
SELECT ROW_NUMBER() OVER (ORDER BY customer_first_name, customer_last_name) as customer_id,
       customer_first_name                                                  as first_name,
       customer_last_name                                                   as last_name,
       CAST(customer_age AS INTEGER)                                        as age,
       customer_email                                                       as email,
       customer_country                                                     as country,
       customer_postal_code                                                 as postal_code,
       customer_pet_type                                                    as pet_type,
       customer_pet_name                                                    as pet_name,
       customer_pet_breed                                                   as pet_breed
FROM unique_customers;

INSERT INTO etl_clickhouse.default.sellers (seller_id,
                                            first_name,
                                            last_name,
                                            email,
                                            country,
                                            postal_code)
WITH combined_sellers AS (SELECT to_utf8(seller_first_name)  as seller_first_name,
                                 to_utf8(seller_last_name)   as seller_last_name,
                                 to_utf8(seller_email)       as seller_email,
                                 to_utf8(seller_country)     as seller_country,
                                 to_utf8(seller_postal_code) as seller_postal_code
                          FROM postgres.public.main_data
                          WHERE seller_first_name IS NOT NULL
                            AND seller_last_name IS NOT NULL

                          UNION ALL

                          SELECT seller_first_name,
                                 seller_last_name,
                                 seller_email,
                                 seller_country,
                                 seller_postal_code
                          FROM source_clickhouse.default.main_data
                          WHERE seller_first_name IS NOT NULL
                            AND seller_last_name IS NOT NULL),
     unique_sellers AS (SELECT seller_first_name,
                               seller_last_name,
                               seller_email,
                               seller_country,
                               seller_postal_code
                        FROM combined_sellers
                        GROUP BY seller_first_name,
                                 seller_last_name,
                                 seller_email,
                                 seller_country,
                                 seller_postal_code)
SELECT ROW_NUMBER() OVER (ORDER BY seller_first_name, seller_last_name) as seller_id,
       seller_first_name                                                as first_name,
       seller_last_name                                                 as last_name,
       seller_email                                                     as email,
       seller_country                                                   as country,
       seller_postal_code                                               as postal_code
FROM unique_sellers;

INSERT INTO etl_clickhouse.default.products (product_id,
                                             name,
                                             category,
                                             price,
                                             weight,
                                             color,
                                             size,
                                             brand,
                                             material,
                                             description,
                                             rating,
                                             reviews,
                                             release_date,
                                             expiry_date)
WITH combined_products AS (SELECT to_utf8(product_name)        as product_name,
                                  to_utf8(product_category)    as product_category,
                                  product_price,
                                  product_weight,
                                  to_utf8(product_color)       as product_color,
                                  to_utf8(product_size)        as product_size,
                                  to_utf8(product_brand)       as product_brand,
                                  to_utf8(product_material)    as product_material,
                                  to_utf8(product_description) as product_description,
                                  product_rating,
                                  product_reviews,
                                  product_release_date,
                                  product_expiry_date
                           FROM postgres.public.main_data
                           WHERE product_name IS NOT NULL

                           UNION ALL

                           SELECT product_name,
                                  product_category,
                                  product_price,
                                  product_weight,
                                  product_color,
                                  product_size,
                                  product_brand,
                                  product_material,
                                  product_description,
                                  product_rating,
                                  product_reviews,
                                  product_release_date,
                                  product_expiry_date
                           FROM source_clickhouse.default.main_data
                           WHERE product_name IS NOT NULL),
     unique_products AS (SELECT product_name,
                                product_category,
                                product_price,
                                product_weight,
                                product_color,
                                product_size,
                                product_brand,
                                product_material,
                                product_description,
                                product_rating,
                                product_reviews,
                                product_release_date,
                                product_expiry_date
                         FROM combined_products
                         GROUP BY product_name,
                                  product_category,
                                  product_price,
                                  product_weight,
                                  product_color,
                                  product_size,
                                  product_brand,
                                  product_material,
                                  product_description,
                                  product_rating,
                                  product_reviews,
                                  product_release_date,
                                  product_expiry_date)
SELECT ROW_NUMBER() OVER (ORDER BY product_name) as product_id,
       product_name                              as name,
       product_category                          as category,
       product_price                             as price,
       product_weight                            as weight,
       product_color                             as color,
       product_size                              as size,
       product_brand                             as brand,
       product_material                          as material,
       product_description                       as description,
       product_rating                            as rating,
       product_reviews                           as reviews,
       product_release_date                      as release_date,
       product_expiry_date                       as expiry_date
FROM unique_products;

INSERT INTO etl_clickhouse.default.stores (store_id,
                                           name,
                                           location,
                                           city,
                                           state,
                                           country,
                                           phone,
                                           email)
WITH combined_stores AS (SELECT to_utf8(store_name)     as store_name,
                                to_utf8(store_location) as store_location,
                                to_utf8(store_city)     as store_city,
                                to_utf8(store_state)    as store_state,
                                to_utf8(store_country)  as store_country,
                                to_utf8(store_phone)    as store_phone,
                                to_utf8(store_email)    as store_email
                         FROM postgres.public.main_data
                         WHERE store_name IS NOT NULL

                         UNION ALL

                         SELECT store_name,
                                store_location,
                                store_city,
                                store_state,
                                store_country,
                                store_phone,
                                store_email
                         FROM source_clickhouse.default.main_data
                         WHERE store_name IS NOT NULL),
     unique_stores AS (SELECT store_name,
                              store_location,
                              store_city,
                              store_state,
                              store_country,
                              store_phone,
                              store_email
                       FROM combined_stores
                       GROUP BY store_name,
                                store_location,
                                store_city,
                                store_state,
                                store_country,
                                store_phone,
                                store_email)
SELECT ROW_NUMBER() OVER (ORDER BY store_name) as store_id,
       store_name                              as name,
       store_location                          as location,
       store_city                              as city,
       store_state                             as state,
       store_country                           as country,
       store_phone                             as phone,
       store_email                             as email
FROM unique_stores;

INSERT INTO etl_clickhouse.default.suppliers (supplier_id,
                                              name,
                                              contact,
                                              email,
                                              phone,
                                              address,
                                              city,
                                              country)
WITH combined_suppliers AS (SELECT to_utf8(supplier_name)    as supplier_name,
                                   to_utf8(supplier_contact) as supplier_contact,
                                   to_utf8(supplier_email)   as supplier_email,
                                   to_utf8(supplier_phone)   as supplier_phone,
                                   to_utf8(supplier_address) as supplier_address,
                                   to_utf8(supplier_city)    as supplier_city,
                                   to_utf8(supplier_country) as supplier_country
                            FROM postgres.public.main_data
                            WHERE supplier_name IS NOT NULL

                            UNION ALL

                            SELECT supplier_name,
                                   supplier_contact,
                                   supplier_email,
                                   supplier_phone,
                                   supplier_address,
                                   supplier_city,
                                   supplier_country
                            FROM source_clickhouse.default.main_data
                            WHERE supplier_name IS NOT NULL),
     unique_suppliers AS (SELECT supplier_name,
                                 supplier_contact,
                                 supplier_email,
                                 supplier_phone,
                                 supplier_address,
                                 supplier_city,
                                 supplier_country
                          FROM combined_suppliers
                          GROUP BY supplier_name,
                                   supplier_contact,
                                   supplier_email,
                                   supplier_phone,
                                   supplier_address,
                                   supplier_city,
                                   supplier_country)
SELECT ROW_NUMBER() OVER (ORDER BY supplier_name) as supplier_id,
       supplier_name                              as name,
       supplier_contact                           as contact,
       supplier_email                             as email,
       supplier_phone                             as phone,
       supplier_address                           as address,
       supplier_city                              as city,
       supplier_country                           as country
FROM unique_suppliers;

INSERT INTO etl_clickhouse.default.fact_sales (sale_id, customer_id, seller_id, product_id, store_id, supplier_id,
                                               sale_date, quantity, total_price)
WITH sales_data AS (SELECT id                        as source_id,
                           to_utf8(customer_email)   as customer_email,
                           to_utf8(seller_email)     as seller_email,
                           to_utf8(product_name)     as product_name,
                           to_utf8(product_category) as product_category,
                           product_price,
                           product_release_date,
                           product_expiry_date,
                           to_utf8(store_name)       as store_name,
                           to_utf8(store_city)       as store_city,
                           to_utf8(supplier_name)    as supplier_name,
                           to_utf8(supplier_email)   as supplier_email,
                           sale_date,
                           sale_quantity,
                           sale_total_price
                    FROM postgres.public.main_data
                    WHERE sale_date IS NOT NULL
                      AND customer_email IS NOT NULL
                      AND seller_email IS NOT NULL
                      AND product_name IS NOT NULL
                      AND store_name IS NOT NULL
                      AND supplier_name IS NOT NULL

                    UNION ALL

                    SELECT id as source_id,
                           customer_email,
                           seller_email,
                           product_name,
                           product_category,
                           product_price,
                           product_release_date,
                           product_expiry_date,
                           store_name,
                           store_city,
                           supplier_name,
                           supplier_email,
                           sale_date,
                           sale_quantity,
                           sale_total_price
                    FROM source_clickhouse.default.main_data
                    WHERE sale_date IS NOT NULL
                      AND customer_email IS NOT NULL
                      AND seller_email IS NOT NULL
                      AND product_name IS NOT NULL
                      AND store_name IS NOT NULL
                      AND supplier_name IS NOT NULL)
SELECT ROW_NUMBER() OVER (ORDER BY sd.sale_date, sd.source_id) as sale_id,
       c.customer_id,
       s.seller_id,
       p.product_id,
       st.store_id,
       sup.supplier_id,
       sd.sale_date,
       CAST(sd.sale_quantity AS INTEGER)                       as quantity,
       CAST(sd.sale_total_price AS DECIMAL(10, 2))             as total_price
FROM sales_data sd
         JOIN etl_clickhouse.default.customers c
              ON from_utf8(sd.customer_email) = from_utf8(c.email)
         JOIN etl_clickhouse.default.sellers s
              ON from_utf8(sd.seller_email) = from_utf8(s.email)
         JOIN etl_clickhouse.default.products p
              ON from_utf8(sd.product_name) = from_utf8(p.name)
                  AND from_utf8(sd.product_category) = from_utf8(p.category)
                  AND sd.product_price = p.price
                  AND sd.product_release_date = p.release_date
                  AND sd.product_expiry_date = p.expiry_date
         JOIN etl_clickhouse.default.stores st
              ON from_utf8(sd.store_name) = from_utf8(st.name)
                  AND from_utf8(sd.store_city) = from_utf8(st.city)
         JOIN etl_clickhouse.default.suppliers sup
              ON from_utf8(sd.supplier_name) = from_utf8(sup.name)
                  AND from_utf8(sd.supplier_email) = from_utf8(sup.email);