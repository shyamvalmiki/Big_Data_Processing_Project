"""Q_4 Gas Guzzler
"""
from pyspark.sql.functions import desc, sum
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import count

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Q1_Project")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            float(fields[9])
            float(fields[8])
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = lines.filter(good_line)
################################################################################
#finding the avg gas_price per month.
    gas_price = clean_lines.map(lambda b: (datetime.fromtimestamp(int(b.split(',')[11])).strftime("%m-%Y"),float(b.split(',')[9])))
    
    sum_count = gas_price.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # compute the average by dividing the sum by the count
    average_gas_price = sum_count.mapValues(lambda x: x[0] / x[1])
    print('################################')
    print(average_gas_price.take(5))
    print('################################')
################################################################################
#finding average gas used per month.
    gas_used = clean_lines.map(lambda b: (datetime.fromtimestamp(int(b.split(',')[11])).strftime("%m-%Y"),float(b.split(',')[8])))
    
    sum_count = gas_used.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # compute the average by dividing the sum by the count
    average_gas_used = sum_count.mapValues(lambda x: x[0] / x[1])
    
    print('################################')
    print(average_gas_used.take(5))
    print('################################')
  
    ##############################################################################################
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Q4_Project' + date_time + '/gaspricepermonth1.txt')
    my_result_object.put(Body=json.dumps(average_gas_price.take(42)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'Q4_Project' + date_time + '/gasusedpermonth1.txt')
    my_result_object.put(Body=json.dumps(average_gas_used.take(42)))
   

    


    spark.stop()