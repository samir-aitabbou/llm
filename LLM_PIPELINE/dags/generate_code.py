
#Step 1: Read Data from Cassandra with Spark

from pyspark.sql import SparkSession
from transformers import AutoTokenizer, pipeline
# import torch


# def create_spark_session():
#     spark = SparkSession.builder \
#         .appName('ReadFromCassandra') \
#         .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
#                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
#         .config('spark.cassandra.connection.host', 'cassandra') \
#         .getOrCreate()
#     return spark

def create_spark_session():
    spark = SparkSession.builder \
        .appName('ReadFromCassandra') \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config('spark.cassandra.connection.host', 'localhost') \
        .getOrCreate()
    return spark

def read_data_from_cassandra(spark, table, keyspace="spark_streams"):
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df

# Example usage:
spark = create_spark_session()
data_df = read_data_from_cassandra(spark, "humaneval_data")
# data_df.show()

#Step 2: Extract Data for LLM


prompt_data = data_df.select("prompt").first()['prompt']
print(prompt_data)


# # Step 3: Use the Extracted Data in LLM

def load_llm_model():
    model = "codellama/CodeLlama-7b-hf"
    tokenizer = AutoTokenizer.from_pretrained(model)
    llm_pipeline = pipeline(
        "text-generation",
        model=model,
        torch_dtype=torch.float16,
        device_map="auto",
    )
    return tokenizer, llm_pipeline

def generate_text(tokenizer, llm_pipeline, prompt):
    sequences = llm_pipeline(
        prompt,
        do_sample=True,
        top_k=10,
        temperature=0.1,
        top_p=0.95,
        num_return_sequences=1,
        eos_token_id=tokenizer.eos_token_id,
        max_length=200,
    )
    for seq in sequences:
        print(f"Result: {seq['generated_text']}")

# # Example usage:
# tokenizer, llm_pipeline = load_llm_model()
# generate_text(tokenizer, llm_pipeline, prompt_data)
