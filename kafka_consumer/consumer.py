# Import necessary libraries
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType

spark = (SparkSession.builder
            .appName("ElasticsearchSparkIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.17.14,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")
            #.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

# Define the Kafka topics
users_topic = "users_topic"
movies_topic = "movies_topic"
ratings_topic = "ratings_topic"

# Define the schema for the Kafka messages
users_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("age", IntegerType()),
    StructField("sex", StringType()),
    StructField("occupation", StringType()),
    StructField("zip_code", StringType())
])

movies_schema = StructType([
    StructField("movie_id", IntegerType()),
    StructField("title", StringType()),
    StructField("release_date", StringType()),
    StructField("imdb_url", StringType()),
    StructField("genres", ArrayType(StringType()))
])

ratings_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("movie_id", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("unix_timestamp", StringType())
])

# Read data from Kafka topics into Spark DataFrames
users_stream_df = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", "localhost:9092")
                   .option("subscribe", users_topic)
                   .load()
                   .selectExpr("CAST(value AS STRING)")
                   .select(from_json("value", users_schema).alias("data"))
                   .select("data.*"))

movies_stream_df = (spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", movies_topic)
                    .load()
                    .selectExpr("CAST(value AS STRING)")
                    .select(from_json("value", movies_schema).alias("data"))
                    .select("data.*"))

ratings_stream_df = (spark
                     .readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", "localhost:9092")
                     .option("subscribe", ratings_topic)
                     .load()
                     .selectExpr("CAST(value AS STRING)")
                     .select(from_json("value", ratings_schema).alias("data"))
                     .select("data.*"))

def create_query(stream_df):
    return (stream_df
            .writeStream
            .outputMode("append")
            .format("console")
            .start())

# Output to console for testing
users_query = create_query(users_stream_df)
movies_query = create_query(movies_stream_df)
ratings_query = create_query(ratings_stream_df)

# Await termination of the streaming queries
users_query.awaitTermination()
movies_query.awaitTermination()
ratings_query.awaitTermination()