# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from kafka_stream import stream_humaneval_data

# default_args = {
#     'owner': 'Alteca',
#     'start_date': datetime(2024, 4, 20, 10, 0),
#     'catchup': False
# }

# with DAG('humaneval_to_kafka_and_spark',
#          default_args=default_args,
#          description='Stream HumanEval data to Kafka and process with Spark daily',
#         #  schedule_interval='@daily',
#          schedule_interval='0 3 * * *',  # This sets the DAG to run daily at 3:00 AM UTC

#          ) as dag:

#     # Task 1: Stream data to Kafka
#     stream_data_to_kafka = PythonOperator(
#         task_id='stream_data_to_kafka',
#         python_callable=stream_humaneval_data,
#         op_kwargs={'language': 'python'}
#     )

    
#     # Define the task using BashOperator
#     process_data_with_spark = BashOperator(
#     task_id='process_data_with_spark',
#     # bash_command='spark-submit --master spark://localhost:7077 spark_stream.py',
#     # bash_command='pwd',
#     # bash_command='spark-submit --master spark://spark-master:7077 /opt/airflow/dags/spark_stream.py',
#     bash_command='spark-submit \
#                     --master spark://spark-master:7077 \
#                     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
#                     /opt/***/dags/HumainEval_Stream.py',

#     dag=dag,
#     )

#     # process_data_with_spark = BashOperator(
#     # task_id='process_data_with_spark',
#     # bash_command='spark-submit --master spark://localhost:7077 spark_stream.py',
#     # cwd='/opt/airflow/dags',  # Set the current working directory to your dags folder
#     # dag=dag,
#     # )


#     # Setting up task dependencies
#     stream_data_to_kafka >> process_data_with_spark






from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka_stream import stream_humaneval_data
# from HumainEval_Stream import process_kafka_messages
from spark_stream import  run_spark_job

default_args = {
    'owner': 'Alteca',
    'start_date': datetime(2024, 4, 20, 10, 0),
    'catchup': False
}

with DAG('humaneval_to_kafka_and_spark',
         default_args=default_args,
         description='Stream HumanEval data to Kafka and process with Spark daily',
         schedule_interval='0 3 * * *',  # This sets the DAG to run daily at 3:00 AM UTC
         ) as dag:

    # Task 1: Stream data to Kafka
    stream_data_to_kafka = PythonOperator(
        task_id='stream_data_to_kafka',
        python_callable=stream_humaneval_data,
        op_kwargs={'language': 'python'}
    )

    # Task 2: Read data from Kafka using Spark  
    read_data_from_kafka_with_spark = PythonOperator(
        task_id='read_data_from_kafka_with_spark',
        python_callable=run_spark_job,
        dag=dag
    )

    # process_data_with_spark = BashOperator(
    #     task_id='process_data_with_spark',
    #     bash_command='spark-submit \
    #                     --master spark://spark-master:7077 \
    #                     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    #                     /opt/***/dags/spark_stream.py',

    #     dag=dag,
    # )

    # Setting up task dependencies
    stream_data_to_kafka >> read_data_from_kafka_with_spark
