from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0",
                "spark.sql.extensions:org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog:org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.type:hive",
                "spark.sql.catalog.dev:org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.dev.type:hadoop",
                "spark.sql.catalog.dev.warehouse:s3://athena-iceberg-workshop-100jn/iceberg/"
                ) \
        .getOrCreate()

## Create a DataFrame
data = spark.createDataFrame([
 ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
 ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
 ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
 ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
],["id", "creation_date", "last_update_time"])

## Write a DataFrame as a Iceberg dataset to the S3 location
spark.sql("""CREATE TABLE IF NOT EXISTS dev.db.iceberg_table (id string,
creation_date string,
last_update_time string)
USING iceberg
location 's3://athena-iceberg-workshop-100jn/example-prefix/db/iceberg_table'""")

data.writeTo("dev.db.iceberg_table").append()

df = spark.read.format("iceberg").load("dev.db.iceberg_table")
df.show()
