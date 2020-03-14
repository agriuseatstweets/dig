import pickle
import os
from datetime import datetime
from gcsfs import GCSFileSystem
from pyspark.sql import SparkSession
from confluent_kafka import Consumer
from clize import run
from environs import Env

env = Env()

RUBY_DATE = 'E MMM dd HH:mm:ss ZZZZ yyyy'

def read_schema(path):
    fs = GCSFileSystem(project='trollhunters')
    with fs.open(path, 'rb') as f:
        schema = pickle.load(f)
    return schema

def get_consumer():
    kafka_brokers = env.str('KAFKA_BROKERS')
    topic = env.str('DIG_TOPIC')
    group = env.str('DIG_GROUP')
    poll_interval = env.str('KAFKA_POLL_INTERVAL', '1920000')

    c = Consumer({
        'bootstrap.servers': kafka_brokers,
        'group.id': group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
	"max.poll.interval.ms": poll_interval
    })

    c.subscribe([topic])

    return c


# NOTE: change cleanup-failures.ignore to true if causing problems
def build_spark():
    driver_memory = env.str("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = env.str("SPARK_EXECUTOR_MEMORY", "4g")
    log_level = env.str("SPARK_LOG_LEVEL", "INFO")

    spark = SparkSession \
        .builder \
        .master('local[2]') \
        .appName('belly') \
        .config("spark.jars",
                "/home/jupyter/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable",
                "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                "/home/jupyter/work/keys/key.json") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
                "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored",
                "false") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel(log_level)
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    return spark, log

def _get_date(msg):

    datestring = msg.value().decode('utf8')
    date = datetime.strptime(datestring, "%Y-%m-%d").date()
    return date


def get_task(timeout):
    c = get_consumer()
    msg = c.poll(timeout)

    # This should stop the process
    if not msg:
        return None, None

    d = _get_date(msg)
    return d, lambda: c.commit(msg, asynchronous=False)


def write_out(df, outpath, date, partitions):
    # We manually create partitions to allow overwrite mode
    f = os.path.join(outpath, f'datestamp={date}')

    df.dropDuplicates(['id']) \
      .drop('datestamp') \
      .coalesce(partitions) \
      .write \
      .parquet(f, mode='overwrite')


def read_in(spark, schema_path, inpath, date):
    schema = read_schema(schema_path)
    df = spark.read.json(inpath, schema=schema, timestampFormat=RUBY_DATE)
    df = df.where(df.datestamp == date)
    return df


def main():
    spark, log = build_spark()

    datalake_path = env.str('DIG_DATALAKE')
    warehouse_path = env.str('DIG_WAREHOUSE')
    schema_path = env.str('DIG_SCHEMA')
    partitions = env.int('DIG_PARTITIONS')
    poll_timeout = env.int('KAFKA_POLL_TIMEOUT')

    # get one message from kafka
    belly_date, commit = get_task(poll_timeout)
    if not belly_date:
        log.warn(f'Dig found no task')
        return

    # Read in from JSON and write out to Parquet
    df = read_in(spark, schema_path, datalake_path, belly_date)
    write_out(df, warehouse_path, belly_date, partitions)

    # commit task
    commit()

    log.warn(f'Dig finished writing {belly_date} to warehouse.')


if __name__ == '__main__':
    run(main)
