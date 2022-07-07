import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import expr
from pyspark.sql.window import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark import SparkContext
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.window as Window
from pyspark.sql.window import Window
from pyspark.sql import SQLContext
import json
import sys
import argparse
import logging 


def outfilepath(date):
    return f"""/usr/local/airflow/data/output/output_{datetime.datetime.strptime(date,'%Y-%m-%dT%H:%M:%S%z').strftime("%Y%m%d")}.csv"""


def fetchData(file, format): 
    logger.info(f'Reading data from path : {file}')
    
    if format == 'json':
        df = spark.read.json(file)
    else :
        df = sqlContext.read.load(file, format='com.databricks.spark.csv', 
                                         header='true', 
                                         inferSchema='true') 
    logger.info(df.printSchema())
    logger.info(df.count())
    return df



def df_transform(df):
    df_processed = df.withColumn("shares_count",col("shares.count"))  \
                     .select('post_id','shares_count')
    return df_processed



def fixna(df):
    ## fill NA/ None data for orders and voucher amount
    df = df.fillna({'shares_count':0, 'comments':0, 'likes':0})
    return df


def execution_date():
    return datetime.datetime.strptime(date_exec,'%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S")

UDFgetdate = F.udf(execution_date, StringType())


def coalesce_by_row_count(df, desired_rows_per_partition=1000):
    """
    coalesce dataframe to reduce number of partitions, to avoid fragmentation of data
    :return: dataframe coalesced
    """
    count = df.count()
    partitions = int(count / desired_rows_per_partition + 1)
    return df.coalesce(partitions)


def main(data, engagement, date_exec):
    df_data = fetchData(data, format='json')
    df_engagement = fetchData(engagement, format='csv')

    df_data_transform = df_transform(df_data)
    df_joined = df_data_transform.join(df_engagement, ['post_id'], how='full') \
                                 .select('post_id','shares_count','comments', 'likes' ) 

    df_joined = df_joined.withColumn("date", UDFgetdate())
    df_joined_transform = fixna(df_joined)
    logger.info( df_joined_transform.show() )
    output_path = outfilepath(date_exec)
    logger.info(f'writing to processed file to location : {output_path}')
    df_coalesced = coalesce_by_row_count(df_joined_transform)
    df_coalesced.write.mode('overwrite').option('header', 'true').csv(output_path)
    logger.info('<<<<<<<<<<<<<<<<< Completed data processing >>>>>>>>>>>>>>>')
 



def setup_logging():
    """
    Initial setup of logging
    """
    logger_name = 'SPARK:ETL-PROCESS: '
    etlLogger = logging.getLogger('ETL-PIPELINE :LOGGER' ) 
    etlLogger.setLevel(logging.INFO)
    format = '%(asctime)s %(name)-15s %(threadName)-15s %(levelname)-7s ' + logger_name + ' %(message)s'
    #handler = logging.StreamHandler(sys.stderr)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(format))
    etlLogger.addHandler(handler)
    etlLogger.info('<<<<<<<<<<<<<<<<< Start Etl data processing >>>>>>>>>>>>>>>')
    return etlLogger



def get_options(parser, args):
    parsed, extra = parser.parse_known_args(args[1:])

    if extra:
        logger.error('Found unrecognized arguments:', extra)
    return vars(parsed)
       

if __name__ == "__main__":
    ## set spark config parameters
    spark = SparkSession.builder.master("spark://spark-master:7077").appName("spark_dataprocessing"). \
                                config("spark.driver.extraClassPath","/jars/hadoop-common-3.2.0.jar: " \
                                                                     "jars/aws-java-sdk-1.11.30.jar:/ "\
                                                                     "jars/hadoop-aws-3.2.0.jar"). \
                                getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    global sqlContext
    sqlContext = SQLContext(spark.sparkContext)
                                  
    ## enabling s3a jars to read from s3 path 
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com")

    ## arg parser taking params 
    parser = argparse.ArgumentParser(description='Spark pipeline - Etl data processing pipeline !!')
    parser.add_argument('--datafile', required=True, help='provide data file, in json format')
    parser.add_argument('--engagementfile'   , required=True, help='provide engagement file, in csv format')
    parser.add_argument('--execution_date', required=True, help='execution date of the job ')
    
    args = sys.argv
    options = get_options(parser, args)
    datafile =  f"/usr/local/airflow/data/{options['datafile']}"
    engagementfile = f"/usr/local/airflow/data/{options['engagementfile']}"
    
    global logger 
    logger = setup_logging()
    global date_exec
    date_exec = options['execution_date']
    print(f' Task execution date : {date_exec}') 

    main(datafile, engagementfile, date_exec)