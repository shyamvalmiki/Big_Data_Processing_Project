"""Q4
"""
import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Question-4")\
        .getOrCreate()

    def good_line_1(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            str(fields[4])
            str(fields[5])
            str(fields[6])
            str(fields[7])
            str(fields[8])
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
###############################################################################################
    
 
    # Reading lines from csv file
    lines_1 = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_lines_1=lines_1.filter(good_line_1)
    
    sha3_uncles = clean_lines_1.map(lambda l: (l.split(',')[4],len(l.split(',')[4])))
    print(sha3_uncles.take(3))
    sum1 = sha3_uncles.map(lambda x: x[1]).reduce(lambda a, b: a + b)
    print(sum1)
    print('***********************************')
    logs_bloom = clean_lines_1.map(lambda l: (l.split(',')[5],len(l.split(',')[5])))
    print(logs_bloom.take(3))
    sum2 = logs_bloom.map(lambda x: x[1]).reduce(lambda a, b: a + b)
    print(sum2)
    print('***********************************')
    transactions_root = clean_lines_1.map(lambda l: (l.split(',')[6],len(l.split(',')[6])))
    print(transactions_root.take(3))
    sum3 = transactions_root.map(lambda x: x[1]).reduce(lambda a, b: a + b)
    print(sum3)
    print('***********************************')
    state_root = clean_lines_1.map(lambda l: (l.split(',')[7],len(l.split(',')[7])))
    print(state_root.take(3))
    sum4 = state_root.map(lambda x: x[1]).reduce(lambda a, b: a + b)
    print(sum4)
    print('***********************************')
    receipts_root = clean_lines_1.map(lambda l: (l.split(',')[8],len(l.split(',')[8])))
    print(receipts_root.take(3))
    sum5 = receipts_root.map(lambda x: x[1]).reduce(lambda a, b: a + b)
    print(sum5)
    print('***********************************')
    
    sum = sum1+sum2+sum3+sum4+sum5
    print('The size is {} '.format(sum))
    print('###########################################################')
    
############################################################################################# 
    spark.stop()