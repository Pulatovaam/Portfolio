import io
import sys
import uuid

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round

DATA_PATH = f"s3a://startde-raw/raw_items"
TARGET_PATH = f"s3a://startde-project/anastasija-pulatova-fqe3933/seller_items"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob1-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())

def main():
    spark = _spark_session()
    orders_df = spark.read.parquet(DATA_PATH)
    w = Window.orderBy("item_rate")
    orders_df = orders_df. \
    withColumn(
        'returned_items_count', 
        round(col('ordered_items_count')*(1-col('avg_percent_to_sold')/100),0).cast('int')). \
    withColumn(
        'potential_revenue', 
        (col('availability_items_count')+col('ordered_items_count'))*col('item_price')). \
    withColumn(
        'total_revenue', 
        (col('ordered_items_count')+col('goods_sold_count')-col('returned_items_count'))*col('item_price')). \
    withColumn(
        'avg_daily_sales', 
        col('goods_sold_count')/col('days_on_sell')). \
    withColumn(
        'days_to_sold', 
        col('availability_items_count')/col('avg_daily_sales')). \
    withColumn(
        'item_rate_percent', 
        F.percent_rank().over(w))
    orders_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
