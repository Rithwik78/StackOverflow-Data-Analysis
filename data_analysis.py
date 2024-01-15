from pyspark.sql.functions import (
    col, count, desc, explode, ceil, unix_timestamp, window, sum, when, array_contains, lit, split
)

%%configure -f
{
    "conf": {
        "spark.pyspark.python": "python3",
        "spark.pyspark.virtualenv.enabled": "true",
        "spark.pyspark.virtualenv.type":"native",
        "spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"
    }
}

sc.install_pypi_package("pandas==1.1.0")

sc.install_pypi_package("matplotlib==3.0.0")

import matplotlib.pyplot as plt

# path to specific S3 location where the data is sitting

posts_path = 's3://stack-overflow-analysis-bucket/output_parquet_file/'

posts_all = spark.read.parquet(posts_path)

posts_all.printSchema()

# select only cols we will work with and cache it

posts = posts_all.select(
    'id',
    'post_type_id',
    'accepted_answer_id',
    'user_id',
    'creation_date',
    'tags'
).cache()

# Compute the Posts count
posts.count()

# Questions and Answers Count
questions = posts.filter(col('post_type_id') == 1)
answers = posts.filter(col('post_type_id') == 2)

questions.count()

answers.count()

# questions with accepted answer

questions.filter(col('accepted_answer_id').isNotNull())

# distinct number of users:

posts.filter(col('user_id').isNotNull()).select('user_id').distinct().count()

# Computing the response time
response_time = (
    questions.alias('questions')
    .join(answers.alias('answers'), col('questions.accepted_answer_id') == col('answers.id'))
    .select(
        col('questions.id'),
        col('questions.creation_date').alias('question_time'),
        col('answers.creation_date').alias('answer_time')
    )
    .withColumn('response_time', unix_timestamp('answer_time') - unix_timestamp('question_time'))
    .filter(col('response_time') > 0)
    .orderBy('response_time')
)

response_time.show(n=5)

hourly_data = (
    response_time
    .withColumn('hours', ceil(col('response_time') / 3600))
    .groupBy('hours')
    .agg(count('*').alias('cnt'))
    .orderBy('hours')
    .limit(24)
).toPandas()

# See the number of questions answered within each hour
hourly_data.plot(
    x='hours', y='cnt', figsize=(12, 6), 
    title='Response time of questions',
    legend=False,
    kind='bar',
    xlabel='Hour',
    ylabel='Number of answered questions'
)

%matplot plt

# See the time evolution of the number of questions and answeres
posts_grouped = (
    posts
    .filter(col('user_id').isNotNull())
    .groupBy(
        window('creation_date', '1 week')
    )
    .agg(
        sum(when(col('post_type_id') == 1, lit(1)).otherwise(lit(0))).alias('questions'),
        sum(when(col('post_type_id') == 2, lit(1)).otherwise(lit(0))).alias('answers')
    )
    .withColumn('date', col('window.start').cast('date'))
    .orderBy('date')
).toPandas()

posts_grouped.plot(
    x='date', 
    figsize=(12, 6), 
    title='Number of questions/answers per week',
    legend=True,
    xlabel='Date',
    ylabel='Number of answers',
    kind='line'
)

%matplot plt

# Compute number of tags
(
    questions
    .withColumn('tags', split('tags', '><'))
    .selectExpr(
        '*',
        "TRANSFORM(tags, value -> regexp_replace(value, '(>|<)', '')) AS tags_arr"
    )
    .withColumn('tag', explode('tags_arr'))
    .select('tag')
    .distinct()
).count()

# See most popular tags
(
    questions
    .withColumn('tags', split('tags', '><'))
    .selectExpr(
        '*',
        "TRANSFORM(tags, value -> regexp_replace(value, '(>|<)', '')) AS tags_arr"
    )
    .withColumn('tag', explode('tags_arr'))
    .groupBy('tag')
    .agg(count('*').alias('tag_frequency'))
    .orderBy(desc('tag_frequency'))
).show(n=10)

# See the popularity of some tags
spark_tag = (
    questions
    .withColumn('tags', split('tags', '><'))
    .selectExpr(
        '*',
        "TRANSFORM(tags, value -> regexp_replace(value, '(>|<)', '')) AS tags_arr"
    )
    .select('id', 'creation_date', 'tags_arr')
    .filter(array_contains(col('tags_arr'), 'apache-spark') | array_contains(col('tags_arr'), 'apache-spark-sql'))
    .groupBy(
        window('creation_date', "1 week")
    )
    .agg(
        count('*').alias('tag_frequency')
    )
    .withColumn('date', col('window.start').cast('date'))
    .orderBy('date')
).toPandas()

spark_tag.plot(
    x='date', 
    figsize=(12, 6), 
    title='spark/spark-sql tag frequency per week',
    legend=False,
    xlabel='Date',
    ylabel='Number of questions with spark tag',
    kind='line'
)

%matplot plt

posts.unpersist()
