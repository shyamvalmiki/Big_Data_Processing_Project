"""gas guzzler
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
            if len(fields)!=15:
                return False
            #float(fields[6])
            float(fields[7])
            return True
        except:
            return False
        
    def good_line_2(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[8])
            #float(fields[7])
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
    
    
 

    lines_1 = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines_1=lines_1.filter(good_line_1)
    transactions_features=clean_lines_1.map(lambda l: (l.split(',')[8],1))
    print(transactions_features.take(1))
    
       # Step 1: Extracting the first elements of each tuple
    first_elements = transactions_features.map(lambda x: float(x[0]))

    # Step 2: Summing up all the first elements
    sum_first_elements = first_elements.reduce(lambda x, y: x + y)

    # Step 3: Counting the total number of tuples
    count = transactions_features.count()

    # Step 4: Calculating the average
    average_first_elements = sum_first_elements / count

    print("Average of first elements: ", average_first_elements)

######################################
   
    
    spark.stop()