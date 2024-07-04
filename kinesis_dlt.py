# Databricks notebook source
import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *

SCOPE_NAME = "dlt_aws_scope"
CSV_PATH = "dbfs:/FileStore/dlt_aws"
KINESIS_REGION = "us-east-1"
KINESIS_DATA_STREAM = "db-connect"


@dlt.table(table_properties={"pipelines.reset.allowed": "false"})
def kinesis_fitbit_bronze():
  return (spark
          .readStream
          .format("kinesis")
          .option("streamName", KINESIS_DATA_STREAM)
          .option("region", KINESIS_REGION)
          .option("initialPosition", 'earliest')
          .option("awsAccessKey", dbutils.secrets.get(scope = "dlt_aws_scope", key = "awsAccessKey"))
          .option("awsSecretKey", dbutils.secrets.get(scope = "dlt_aws_scope", key = "awsSecretKey"))
          .load())

event_schema = StructType([
    StructField("time", TimestampType(),True), \
    StructField("version", StringType(),True), \
    StructField("model", StringType(),True), \
    StructField('Gender', StringType(),True), \
    StructField("age",IntegerType(),True),\
    StructField("heart_bpm", IntegerType(),True), \
    StructField("cal", IntegerType(),True)])


@dlt.table(comment="real schema for kinesis payload")
def kinesis_fitbit_silver():
  return (
   dlt.read_stream("kinesis_fitbit_bronze")
    .select(col("approximateArrivalTimestamp"),from_json(col("data")
    .cast("string"), event_schema).alias("event"))
    .select("approximateArrivalTimestamp", "event.*")
  )

@dlt.table(comment="calories burned by gender")
def kinesis_fitbit_gold_calories_burned():
  return (
   dlt.read_stream("kinesis_fitbit_silver").groupBy('Gender').agg(sum('cal').alias('calories_burned_by_gender'))
  )

@dlt.table(comment="average heart beat by gender")
def kinesis_fitbit_gold_average_heart_beat():
  return (
   dlt.read_stream("kinesis_fitbit_silver").groupBy('Gender').agg(avg("heart_bpm").alias('average_heart_beat_by_gender'))
  )

# COMMAND ----------


