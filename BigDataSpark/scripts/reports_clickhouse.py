from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Инициализация Spark сессии
spark = SparkSession.builder.appName("ReportsClickHouse").getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

ch_url = "jdbc:clickhouse://clickhouse:8123/default"
ch_driver = "com.clickhouse.jdbc.ClickHouseDriver"

fact = spark.read.jdbc(url=pg_url, table="fact_sales", properties=pg_props)
dim_p = spark.read.jdbc(url=pg_url, table="products", properties=pg_props)
dim_c = spark.read.jdbc(url=pg_url, table="customers", properties=pg_props)
dim_st = spark.read.jdbc(url=pg_url, table="stores", properties=pg_props)
dim_sup = spark.read.jdbc(url=pg_url, table="suppliers", properties=pg_props)

# 1. Витрина продаж по продуктам
top10_products = (
    fact.groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_price").alias("total_revenue")
    )
    .join(dim_p, "product_id")
    .select("product_id", "name", "category", "total_quantity", "total_revenue")
    .orderBy(F.desc("total_quantity"))
    .limit(10)
)

top10_products.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "top10_products") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

revenue_by_category = (
    fact.join(dim_p, "product_id")
    .groupBy("category")
    .agg(F.sum("total_price").alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
)

revenue_by_category.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "revenue_by_category") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

product_ratings = dim_p.select("product_id", "name", "category", "rating", "reviews")

product_ratings.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "product_ratings") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# 2. Витрина продаж по клиентам
top10_customers = (
    fact.groupBy("customer_id")
    .agg(F.sum("total_price").alias("total_spent"))
    .join(dim_c, "customer_id")
    .select("customer_id", "first_name", "last_name", "country", "total_spent")
    .orderBy(F.desc("total_spent"))
    .limit(10)
)

top10_customers.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "top10_customers") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

customers_by_country = (
    dim_c.groupBy("country")
    .agg(F.countDistinct("customer_id").alias("num_customers"))
    .orderBy(F.desc("num_customers"))
)

customers_by_country.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "customers_by_country") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

avg_check_by_customer = (
    fact.groupBy("customer_id")
    .agg((F.sum("total_price") / F.count("quantity")).alias("avg_check"))
    .join(dim_c, "customer_id")
    .select("customer_id", "avg_check")
)

avg_check_by_customer.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "avg_check_by_customer") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# 3. Витрина продаж по времени
monthly_trends = (
    fact.withColumn("year", F.year("sale_date"))
    .withColumn("month", F.month("sale_date"))
    .groupBy("year", "month")
    .agg(
        F.sum("total_price").alias("revenue"),
        F.sum("quantity").alias("quantity")
    )
    .withColumn("avg_order_size", F.col("revenue") / F.col("quantity"))
    .orderBy("year", "month")
)

monthly_trends.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "monthly_trends") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

yearly_trends = (
    fact.withColumn("year", F.year("sale_date"))
    .groupBy("year")
    .agg(
        F.sum("total_price").alias("revenue"),
        F.sum("quantity").alias("quantity")
    )
    .orderBy("year")
)

yearly_trends.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "yearly_trends") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# 4. Витрина продаж по магазинам
top5_stores = (
    fact.groupBy("store_id")
    .agg(F.sum("total_price").alias("revenue"))
    .join(dim_st, "store_id")
    .select("name", "city", "country", "revenue")
    .orderBy(F.desc("revenue"))
    .limit(5)
)

top5_stores.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "top5_stores") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

sales_by_city_country = (
    fact.join(dim_st, "store_id")
    .groupBy("city", "country")
    .agg(
        F.sum("total_price").alias("revenue"),
        F.sum("quantity").alias("quantity")
    )
    .orderBy(F.desc("revenue"))
)

sales_by_city_country.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "sales_by_city_country") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

avg_check_by_store = (
    fact.groupBy("store_id")
    .agg((F.sum("total_price") / F.count("quantity")).alias("avg_check"))
    .join(dim_st, "store_id")
    .select("name", "avg_check")
)

avg_check_by_store.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "avg_check_by_store") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# 5. Витрина продаж по поставщикам
top5_suppliers = (
    fact.groupBy("supplier_id")
    .agg(F.sum("total_price").alias("revenue"))
    .join(dim_sup, "supplier_id")
    .select("name", "city", "country", "revenue")
    .orderBy(F.desc("revenue"))
    .limit(5)
)

top5_suppliers.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "top5_suppliers") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

avg_price_by_supplier = (
    fact.groupBy("supplier_id")
    .agg(F.avg(F.col("total_price") / F.col("quantity")).alias("avg_price"))
    .join(dim_sup, "supplier_id")
    .select("name", "avg_price")
)

avg_price_by_supplier.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "avg_price_by_supplier") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

sales_by_supplier_country = (
    fact.join(dim_sup, "supplier_id")
    .groupBy("country")
    .agg(
        F.sum("total_price").alias("revenue"),
        F.sum("quantity").alias("quantity")
    )
    .orderBy(F.desc("revenue"))
)

sales_by_supplier_country.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "sales_by_supplier_country") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

# 6. Витрина качества продукции
highest_rated = (
    dim_p.orderBy(F.desc("rating"))
    .limit(10)
    .select("product_id", "name", "rating")
)

highest_rated.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "highest_rated_products") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

lowest_rated = (
    dim_p.orderBy(F.asc("rating"))
    .limit(10)
    .select("product_id", "name", "rating")
)

lowest_rated.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "lowest_rated_products") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

rating_sales = (
    fact.join(dim_p, "product_id")
    .groupBy("product_id", "name")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum("quantity").alias("total_quantity")
    )
)

corr_value = rating_sales.stat.corr("avg_rating", "total_quantity")
corr_df = spark.createDataFrame([(corr_value,)], ["rating_sales_correlation"])

corr_df.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "rating_sales_correlation") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

top_reviewed = (
    dim_p.orderBy(F.desc("reviews"))
    .limit(10)
    .select("product_id", "name", "reviews")
)

top_reviewed.write.format("jdbc").mode("overwrite") \
    .option("url", ch_url) \
    .option("dbtable", "top_reviewed_products") \
    .option("driver", ch_driver) \
    .option("createTableOptions", "ENGINE = Log") \
    .save()

spark.stop()