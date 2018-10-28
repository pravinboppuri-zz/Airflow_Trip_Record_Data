import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as func
from pyspark.sql.functions import *
import os

# Airflow to schduele my ETL

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

sc= SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# sqlContext to read source file
source_df = sqlContext.read.load('/home/bella/airflow/yellow_tripdata_2017-02.csv',
                                format='com.databricks.spark.csv',
                                header='true',
                                 inferSchema='true')

# Creating Dataframes by selecting only few columns for the analysis
DF1 = source_df.select(source_df['trip_distance'],month(source_df['tpep_dropoff_datetime']).alias('month'),year(source_df['tpep_dropoff_datetime']).alias('year'))
DF2 = source_df.select(source_df['trip_distance'],month(source_df['tpep_dropoff_datetime']).alias('month'),year(source_df['tpep_dropoff_datetime']).alias('year'),source_df['tpep_dropoff_datetime'])


def monthly_avg():
    monthly_avg = DF1.groupby('month', 'year') \
        .agg(func.avg('trip_distance').alias('avg_trip_distance')) \
        .orderBy('month', 'year')

    monthly_avg.createOrReplaceTempView("m_avg")

    df_mavg= sqlContext.sql("select * from m_avg where month=1 and year=1")
    return df_mavg
  
# using pyspark window function to calling rolling average
def rolling_avg():
  
  #function to calculate no: of seconds from no: of days
    days = lambda i: i * 86400
    window_spec = Window.orderBy(func.col("tpep_dropoff_datetime").cast('long')).rangeBetween(-days(45), 0)
    rolling_avg = DF2.withColumn('rolling_avg', func.avg('trip_distance').over(window_spec))

    rolling_avg.createOrReplaceTempView("r_avg")

    df_rolling_avg=sqlContext.sql("select * from r_avg where month=1 and year=2017 limit 10")
    return df_rolling_avg

# Writing the spark dataframes into csv's
def get_averages():

    DF_monthly_avg=monthly_avg()
    DF_rolling_avg=rolling_avg()

    path_monthly_avg = '/home/bella/Yellow_taxi_monthly/'
    path_rolling_avg = '/home/bella/Yellow_taxi_rolling/'
    _delimiter=','

    _filename1 = DF_monthly_avg.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').option('delimiter', _delimiter).option('parserLib', '').mode("overwrite").save(path_monthly_avg)
    _filename2 = DF_rolling_avg.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').option('delimiter', _delimiter).option('parserLib', '').mode("overwrite").save(path_rolling_avg)

default_args = {
    'owner': 'pravin boppuri',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 28),
    'retries': 1,
    'email': ['pravinboppuri@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=30)
}

dag = DAG(dag_id='rides-airflow',
          default_args=default_args,
          schedule_interval=timedelta(1),
          catchup=False)


get_averages_task = \
    PythonOperator(task_id='get_averages',
                   provide_context=True,
                   python_callable=get_averages,
dag=dag)

