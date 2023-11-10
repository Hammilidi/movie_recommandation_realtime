from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, FloatType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("movielensApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Read from Kafka
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movielens-data") \
    .load()

# Define the StructType for the movie data
movie_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("results", ArrayType(
        StructType([
            StructField("adult", BooleanType(), True),
            StructField("backdrop_path", StringType(), True),
            StructField("genre_ids", ArrayType(IntegerType()), True),
            StructField("id", IntegerType(), True),
            StructField("original_language", StringType(), True),
            StructField("original_title", StringType(), True),
            StructField("overview", StringType(), True),
            StructField("popularity", FloatType(), True),
            StructField("poster_path", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("title", StringType(), True),
            StructField("video", BooleanType(), True),
            StructField("vote_average", FloatType(), True),
            StructField("vote_count", IntegerType(), True)
        ])
    ), True)
])

# Parse Kafka messages and apply schema
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], movie_schema))

# Explode the results array
df = df.select("values.page", explode("values.results").alias("movie_data"))

# Access fields within the struct
df = df.select("page", "movie_data.*")

# Write the streaming DataFrame to the console
query = df.writeStream.outputMode("append").format("console").start()

# Wait for the termination of the query
query.awaitTermination()
