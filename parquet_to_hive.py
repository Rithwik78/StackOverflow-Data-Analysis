from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Set the job parameters directly
EMR_CLUSTER_MASTER = "ec2-13-57-242-187.us-west-1.compute.amazonaws.com" # Replace with your EMR cluster's master address
HIVE_DATABASE_NAME = "hive_stack_oveflow_db"
S3_PARQUET_FOLDER = "s3://stack-overflow-analysis-bucket/output_parquet_file/"

# Set up the GlueContext and SparkContext objects
glueContext = GlueContext(SparkContext.getOrCreate())

# Create a new Spark session with the necessary configurations to connect to the EMR cluster
spark = (SparkSession.builder
         .appName("EMR Hive Table Creation")
         .config("spark.master", f"yarn")
         .config("spark.submit.deployMode", "client")
         .config("spark.hadoop.yarn.resourcemanager.address", f"{EMR_CLUSTER_MASTER}:8032")
         .config("spark.hadoop.fs.defaultFS", f"hdfs://{EMR_CLUSTER_MASTER}:8020")
         .config("spark.hadoop.fs.s3a.access.key", "AKIASAGXVPROKBLUNY55") # Replace with your AWS Access Key
         .config("spark.hadoop.fs.s3a.secret.key", "ko8gVJU+4j80HiOt2+VJJ2+3pAkxfHCAOCavWqqA") # Replace with your AWS Secret Key
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .getOrCreate())

# Create a new Hive database if it doesn't already exist
hive_database_name = HIVE_DATABASE_NAME
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database_name}")
spark.sql(f"USE {hive_database_name}")

# Define the column definitions, replace these with your actual column names and data types
column_definitions = """
    id STRING,
    creation_date TIMESTAMP,
    title STRING,
    body STRING,
    comments LONG,
    accepted_answer_id LONG,
    answers LONG,
    favorite_count LONG,
    owner_display_name STRING,
    user_id LONG,
    parent_id LONG,
    post_type_id LONG,
    score LONG,
    tags STRING,
    views LONG
"""

spark.sql("DROP TABLE IF EXISTS hive_stack_oveflow_db.hive_stack_oveflow_table")

# Create an external table in Hive that references the Parquet files in S3
spark.sql(f"""
CREATE EXTERNAL TABLE hive_stack_oveflow_table ({column_definitions})
STORED AS PARQUET
LOCATION '{S3_PARQUET_FOLDER}'
""")