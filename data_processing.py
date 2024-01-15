%%configure -f
{
    "conf": {
        "spark.jars.packages": "com.databricks:spark-xml_2.12:0.14.0"
    }
}

from pyspark.sql.functions import col

# path to the posts dataset in raw format (XML)
posts_input_path = 's3://stack-overflow-analysis-bucket/raw_data_dump/'

# output path where we save the posts in parquet format
posts_output_path = 's3://stack-overflow-analysis-bucket/output_parquet_file/'

(
    spark
    .read
    .format('xml')
    .option('samplingRatio', 0.01)
    .option("rowTag", "row")
    .load(posts_input_path)
    .select(
        col('_Id').alias('id'),
        (col('_CreationDate').cast('timestamp')).alias('creation_date'),
        col('_Title').alias('title'),
        col('_Body').alias('body'),
        col('_commentCount').alias('comments'),
        col('_AcceptedAnswerId').alias('accepted_answer_id'),
        col('_AnswerCount').alias('answers'),
        col('_FavoriteCount').alias('favorite_count'),
        col('_OwnerDisplayName').alias('owner_display_name'),
        col('_OwnerUserId').alias('user_id'),
        col('_ParentId').alias('parent_id'),
        col('_PostTypeId').alias('post_type_id'),
        col('_Score').alias('score'),
        col('_Tags').alias('tags'),
        col('_ViewCount').alias('views')
    )
    .write
    .mode('overwrite')
    .format('parquet')
    .option('path', posts_output_path)
    .save()
)
