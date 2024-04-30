import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


# Configure logging at the beginning of your script
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        msg = "Keyspace created successfully!"
        print(msg)
        logging.info(msg)
    except Exception as e:
        logging.error(" ********* Failed to create keyspace: %s *********", e)
        print(f"********* Failed to create keyspace: {e} *********")

def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.humaneval_data (
            task_id TEXT PRIMARY KEY,
            prompt TEXT,
            declaration TEXT,
            canonical_solution TEXT,
            buggy_solution TEXT,
            bug_type TEXT,
            failure_symptoms TEXT,
            entry_point TEXT,
            import_ TEXT,
            test_setup TEXT,
            test TEXT,
            example_test TEXT,
            signature TEXT,
            docstring TEXT,
            instruction TEXT
        );
        """)
        msg = "Table created successfully!"
        print(msg)
        logging.info(msg)
    except Exception as e:
        logging.error("********* Failed to create table: %s *********", e)
        print(f" ********* Failed to create table: {e} *********")


def insert_data(session, item):
    try:
        session.execute("""
            INSERT INTO spark_streams.humaneval_data (
                task_id, prompt, declaration, canonical_solution, buggy_solution, 
                bug_type, failure_symptoms, entry_point, import_, test_setup, 
                test, example_test, signature, docstring, instruction)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (item['task_id'], item['prompt'], item['declaration'], item['canonical_solution'], item['buggy_solution'],
              item['bug_type'], item['failure_symptoms'], item['entry_point'], item['import_'], item['test_setup'],
              item['test'], item['example_test'], item['signature'], item['docstring'], item['instruction']))
        logging.info(f"********* Data inserted for task_id: {item['task_id']} *********")
    except Exception as e:
        logging.error(f'********* Could not insert data due to {e} *********')


def create_spark_connection():
    s_conn = None
    
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming_From_Kafka') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate() # henaaaaaaaaa bedlttt  localhost --> cassandra

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("************* Spark connection created successfully! *********")
    except Exception as e:
        logging.error(f" ********* Couldn't create the spark session due to exception {e} ********* ")

    return s_conn

    # henaaaaaaaaa bedlttt localhost:9092 --> kafka-broker:29092
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka-broker:29092') \
            .option('subscribe', 'human_eval_data') \
            .option('startingOffsets', 'earliest') \
            .load()             
        logging.info(" ********* kafka dataframe created successfully ********* ")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster

            # henaaaaaaaaa bedlttt localhost:9092 --> kafka-broker:29092 cluster = Cluster(['localhost'])

        # cluster = Cluster(['localhost'])
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"********* Could not create cassandra connection due to {e} ********* ")
        return None


def create_selection_df_from_kafka(spark_df):
    try:
        schema = StructType([
            StructField("task_id", StringType(), True),
            StructField("prompt", StringType(), True),
            StructField("declaration", StringType(), True),
            StructField("canonical_solution", StringType(), True),
            StructField("buggy_solution", StringType(), True),
            StructField("bug_type", StringType(), True),
            StructField("failure_symptoms", StringType(), True),
            StructField("entry_point", StringType(), True),
            StructField("import_", StringType(), True),  # Renamed to avoid keyword conflict
            StructField("test_setup", StringType(), True),
            StructField("test", StringType(), True),
            StructField("example_test", StringType(), True),
            StructField("signature", StringType(), True),
            StructField("docstring", StringType(), True),
            StructField("instruction", StringType(), True),
        ])

        # Parse the JSON from Kafka and select the data field
        # selection_df = spark_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        # logging.info(" ********* DataFrame creation from Kafka successful *********")
        # return selection_df

        sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

        logging.info(" ********* DataFrame creation from Kafka successful *********")

        print(sel)

        return sel
    
    except Exception as e:
        logging.error(" ********* Error creating DataFrame from Kafka: %s ********* ", e)
        raise  # It's important to raise the exception to not mask errors.


# if __name__ == "__main__":  

def run_spark_job():
        
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)

            print("*************** Streaming is being started... **********")
            streaming_query = (selection_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .option("keyspace", "spark_streams") \
                .option("table", "humaneval_data") \
                .start())

            streaming_query.awaitTermination()
    else:
        logging.error(" ********* spark conection empty !!!!!!!!!! ********* ")

