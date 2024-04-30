
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

import logging

# Configure logging at the beginning of your script
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


# Define a global variable to hold the Spark session
spark = None


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('test_spark_streaming') \
            .master("spark://spark-master:7077") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def verify_kafka_data():
    spark = create_spark_connection()

    # Define the schema of your data if you need to parse JSON from Kafka messages
    schema = StructType([
        StructField("task_id", StringType(), True),
        StructField("prompt", StringType(), True),
        StructField("declaration", StringType(), True),
        StructField("canonical_solution", StringType(), True),
        StructField("buggy_solution", StringType(), True),
        StructField("bug_type", StringType(), True),
        StructField("failure_symptoms", StringType(), True),
        StructField("entry_point", StringType(), True),
        StructField("import_", StringType(), True),
        StructField("test_setup", StringType(), True),
        StructField("test", StringType(), True),
        StructField("example_test", StringType(), True),
        StructField("signature", StringType(), True),
        StructField("docstring", StringType(), True),
        StructField("instruction", StringType(), True)
    ])
               
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-broker:29092") \
        .option("subscribe", "human_eval_data") \
        .option("startingOffsets", "latest") \
        .load()

    # Select the value of the Kafka message and cast it to string
    # Optionally, you can parse the JSON if your Kafka message is in JSON format
    string_df = kafka_df.select(col("value").cast("string"))

    # If your messages are JSON strings and you want to parse them,
    # you can use the from_json function with the schema

    # json_df = string_df.select(from_json(col("value").cast("string"), schema).alias("data"))

    # Output the raw string or the parsed JSON to the console
    # Use json_df if you are parsing JSON, otherwise use string_df
    query = string_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    # streaming_query = (string_df.writeStream \
    # .format("org.apache.spark.sql.cassandra") \
    # .option("checkpointLocation", "/tmp/checkpoint") \
    # .option("keyspace", "spark_streams") \
    # .option("table", "humaneval_data") \
    # .start())

    query.awaitTermination()

if __name__ == "__main__":
    verify_kafka_data()
