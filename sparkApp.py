import findspark
from pyspark.sql.functions import col, regexp_replace
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
from pyspark.sql.functions import sha2,concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, FloatType, BinaryType, BooleanType
from pyspark.sql.functions import from_json, explode, concat_ws, when, col, current_date, datediff
from pyspark.sql import DataFrame
import pyspark.sql.functions as F







findspark.init()

# Initialiser une session Spark
spark = SparkSession.builder.appName("movielensApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


# Lire depuis Kafka
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movielens-data") \
    .load()
 
# Définir un schéma pour les données entrantes

# Define the schema for the movie data
schema = StructType([
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
    StructField("vote_count", IntegerType(), True),
])

# Analyser les messages Kafka et appliquer le schéma
parsed_stream = kafkaStream.selectExpr("CAST(value AS STRING)")
# Assuming you have defined 'parsed_stream' DataFrame earlier

df = parsed_stream.withColumn("values", from_json(parsed_stream["value"], schema))

# Access fields within the struct
df = df.select("values.*")

# Write the streaming DataFrame to the console
query = df.writeStream.outputMode("append").format("console").start()

# Wait for the termination of the query
query.awaitTermination()

