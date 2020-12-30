import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', TimestampType(), True),
    StructField('call_date', TimestampType(), True),
    StructField('offense_date', TimestampType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])

def run_spark_job(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:8082") \
        .option("subscribe", "police-calls") \
        .option("startingOffsets", "earliest") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr('CAST(value AS STRING)')

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    service_table.printSchema()

    # select original_crime_type_name and disposition
    distinct_table = service_table \
            .select("original_crime_type_name","disposition","call_date_time") \
            .distinct() \
            .withWatermark('call_date_time', "1 minute")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count()

    query = agg_df\
            .writeStream\
            .queryName('agg_df') \
            .format('console') \
            .outputMode('Complete') \
            .option("truncate", "false") \
            .start() \
            .awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = agg_df \
                    .join(radio_code_df, col('agg_df.disposition') == col('radio_code_df.disposition')) \
                    .writeStream \
                    .queryName('join_query') \
                    .format('console') \
                    .outputMode('complete') \
                    .start() \
                    .awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config('spark.ui.port', 3000) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')
    logger.info("Spark started")
    run_spark_job(spark)
    spark.stop()
