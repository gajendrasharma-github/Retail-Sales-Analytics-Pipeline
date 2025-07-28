import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load CSV from S3
df = spark.read.option("header", "true").csv("s3://retaildataproject/raw/retail_sales_data.csv")

# Cast data types
df_cleaned = df.withColumn("Quantity_Sold", df["Quantity_Sold"].cast("int")) \
               .withColumn("Revenue", df["Revenue"].cast("double")) \
               .withColumn("Date", df["Date"].cast("date"))

# Aggregate revenue by Date and Product
df_agg = df_cleaned.groupBy("Date", "Product") \
                   .sum("Quantity_Sold", "Revenue") \
                   .withColumnRenamed("sum(Quantity_Sold)", "Total_Quantity") \
                   .withColumnRenamed("sum(Revenue)", "Total_Revenue")

# Write result to S3 in Parquet format
df_agg.write.mode("overwrite").parquet("s3://retaildataproject/processed/")

job.commit()
