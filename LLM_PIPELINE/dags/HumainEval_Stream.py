import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('test_spark_streaming') \
            .master("spark://localhost:7077") \
            .config("spark.ui.port", "4050") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info(" **********Spark connection created successfully! **********")
        print("********** Spark session created successfully. **********")
    except Exception as e:
        logging.error(f"********** Couldn't create the spark session due to exception {e} **********")
        print(f"********** Failed to create spark session: {e} **********")

    return s_conn

def process_kafka_messages():
    spark_conn = create_spark_connection()
    if not spark_conn:
        logging.error("********** Spark connection could not be established. **********")
        print(" **********Failed to establish spark connection. **********")
        return  # Early return to avoid further errors

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

    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'human_eval_data') \
            .option('startingOffsets', 'earliest') \
            .load()             
        logging.info(" **********Kafka dataframe created successfully. **********")
        print(" ********** Kafka dataframe created successfully. **********")
    except Exception as e:
        logging.error(f" ********** Failed to create Kafka dataframe because: {e} **********")
        print(f" ********** Failed to create Kafka dataframe: {e} ********** ")
        return  # Early return

    json_df = spark_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
    print(" ********** hehoooooo 1 **********")


    def process_batch(df, epoch_id):
        # Convert the DataFrame to a Pandas DataFrame and print it out
        print("Batch ID:", epoch_id)
        pandas_df = df.toPandas()
        print(pandas_df)

    # Use foreachBatch instead of foreach
    query = json_df.writeStream \
        .foreachBatch(process_batch) \
        .start()

    import time
    while query.isActive:
        print("Query Status: ", query.status)
        print("Recent Progress: ", query.lastProgress)
        time.sleep(10)  # Sleep time in seconds, adjust as necessary

    query.awaitTermination()
    
if __name__ == "__main__":
    process_kafka_messages()



# from pyspark.sql import SparkSession
# import os


# def create_spark_connection():
#     s_conn = SparkSession.builder \
#         .appName('test_spark_streaming') \
#         .master("spark://localhost:7077") \
#         .config("spark.ui.port", "4050") \
#         .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
#         .getOrCreate()
#     s_conn.sparkContext.setLogLevel("ERROR")
#     print("********** Spark session created successfully. **********")
#     return s_conn

# def process_kafka_messages(spark_conn):
#     kafka_df = spark_conn.readStream \
#         .format('kafka') \
#         .option('kafka.bootstrap.servers', 'localhost:9092') \
#         .option('subscribe', 'human_eval_data') \
#         .option('startingOffsets', 'earliest') \
#         .load()

#     # Selecting and casting the 'value' column to a string
#     values_df = kafka_df.selectExpr("CAST(value AS STRING)")
#     # query = values_df.writeStream.foreachBatch(lambda df, epoch_id: print(df.count())).start()


#     # Output the raw data to the console
#     query = values_df.writeStream \
#         .outputMode('append') \
#         .format('console') \
#         .start()
    

#     import time
#     while query.isActive:
#         print("Query Status: ", query.status)
#         print("Recent Progress: ", query.lastProgress)
#         time.sleep(10)  # Sleep time in seconds, adjust as necessary

#     # Output the data to JSON files

#     # Define the path where the files will be stored
#     # home_dir = os.path.expanduser("~")
#     # output_path = os.path.join(home_dir, "streaming_output")
#     # checkpoint_path = os.path.join(home_dir, "streaming_checkpoint")

#     # # Make sure the directories exist or create them
#     # os.makedirs(output_path, exist_ok=True)
#     # os.makedirs(checkpoint_path, exist_ok=True)

#     # # Output the data to JSON files
#     # query = values_df.writeStream \
#     #     .outputMode('append') \
#     #     .format('json') \
#     #     .option("path", output_path) \
#     #     .option("checkpointLocation", checkpoint_path) \
#     #     .start()
    
    

#     query.awaitTermination()

# if __name__ == "__main__":
#     spark_conn = create_spark_connection()
#     process_kafka_messages(spark_conn)
