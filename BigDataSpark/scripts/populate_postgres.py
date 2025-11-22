from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, expr
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("PopulatePostgres")
    .getOrCreate()
)

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_dsn = {
    "user":     "postgres",
    "password": "postgres",
    "driver":   "org.postgresql.Driver"
}

stg = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "main_data")
    .option("user",    pg_dsn["user"])
    .option("password",pg_dsn["password"])
    .option("driver",  pg_dsn["driver"])
    .load()
)

def load_dim(df, partition_col, order_col, selects, renames, target_table):
    win = Window.partitionBy(partition_col).orderBy(order_col)
    df_dim = (
        df
        .select(partition_col, order_col, *selects)
        .withColumn("rn", row_number().over(win))
        .filter(col("rn") == 1)
        .drop("rn", order_col)
    )
    if partition_col in renames:
        df_dim = df_dim.withColumnRenamed(partition_col, renames[partition_col])
        renames = {k: v for k, v in renames.items() if k != partition_col}

    for old, new in renames.items():
        df_dim = df_dim.withColumnRenamed(old, new)
    df_dim.write \
        .mode("append") \
        .jdbc(pg_url, target_table, properties=pg_dsn)


load_dim(
    stg,
    partition_col="customer_email",
    order_col="sale_date",
    selects=[
        "customer_first_name", "customer_last_name",
        "customer_age", "customer_country", "customer_postal_code",
        "customer_pet_type", "customer_pet_name", "customer_pet_breed"
    ],
    renames={
        "customer_email":      "email",
        "customer_first_name": "first_name",
        "customer_last_name":  "last_name",
        "customer_age":        "age",
        "customer_country":    "country",
        "customer_postal_code":"postal_code",
        "customer_pet_type":   "pet_type",
        "customer_pet_name":   "pet_name",
        "customer_pet_breed":  "pet_breed"
    },
    target_table="customers"
)

load_dim(
    stg,
    partition_col="seller_email",
    order_col="sale_date",
    selects=[
        "seller_first_name", "seller_last_name",
        "seller_country", "seller_postal_code"
    ],
    renames={
        "seller_email":      "email",
        "seller_first_name": "first_name",
        "seller_last_name":  "last_name",
        "seller_country":    "country",
        "seller_postal_code":"postal_code"
    },
    target_table="sellers"
)

load_dim(
    stg,
    partition_col="sale_product_id",
    order_col="sale_date",
    selects=[
        "product_name", "product_category", "product_price", "product_weight",
        "product_color", "product_size", "product_brand",
        "product_material", "product_description",
        "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date"
    ],
    renames={
        "sale_product_id":      "product_id",
        "product_name":         "name",
        "product_category":     "category",
        "product_price":        "price",
        "product_weight":       "weight",
        "product_color":        "color",
        "product_size":         "size",
        "product_brand":        "brand",
        "product_material":     "material",
        "product_description":  "description",
        "product_rating":       "rating",
        "product_reviews":      "reviews",
        "product_release_date": "release_date",
        "product_expiry_date":  "expiry_date"
    },
    target_table="products"
)

load_dim(
    stg,
    partition_col="store_name",
    order_col="sale_date",
    selects=[
        "store_location", "store_city", "store_state",
        "store_country", "store_phone", "store_email"
    ],
    renames={
        "store_name":     "name",
        "store_location": "location",
        "store_city":     "city",
        "store_state":    "state",
        "store_country":  "country",
        "store_phone":    "phone",
        "store_email":    "email"
    },
    target_table="stores"
)

load_dim(
    stg,
    partition_col="supplier_name",
    order_col="sale_date",
    selects=[
        "supplier_contact", "supplier_email", "supplier_phone",
        "supplier_address", "supplier_city", "supplier_country"
    ],
    renames={
        "supplier_name":    "name",
        "supplier_contact": "contact",
        "supplier_email":   "email",
        "supplier_phone":   "phone",
        "supplier_address": "address",
        "supplier_city":    "city",
        "supplier_country": "country"
    },
    target_table="suppliers"
)

customers_df  = spark.read.jdbc(pg_url, "customers",  properties=pg_dsn)
sellers_df    = spark.read.jdbc(pg_url, "sellers",    properties=pg_dsn)
products_df   = spark.read.jdbc(pg_url, "products",   properties=pg_dsn)
stores_df     = spark.read.jdbc(pg_url, "stores",     properties=pg_dsn)
suppliers_df  = spark.read.jdbc(pg_url, "suppliers",  properties=pg_dsn)

fact_sales = (
    stg
    .join(customers_df, stg.customer_email == customers_df.email)
    .join(sellers_df,   stg.seller_email == sellers_df.email)
    .join(products_df,  stg.sale_product_id == products_df.product_id)
    .join(stores_df,    stg.store_name == stores_df.name)
    .join(suppliers_df, stg.supplier_name == suppliers_df.name)
    .select(
        customers_df.customer_id.alias("customer_id"),
        sellers_df.seller_id.alias("seller_id"),
        products_df.product_id.alias("product_id"),
        stores_df.store_id.alias("store_id"),
        suppliers_df.supplier_id.alias("supplier_id"),
        col("sale_date"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    )
)

fact_sales.write \
    .mode("append") \
    .jdbc(pg_url, "fact_sales", properties=pg_dsn)

print("Data loading completed successfully!")
spark.stop()