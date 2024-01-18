"""Question - 2
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
        .appName("Question-2")\
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
            if len(fields)!=6:
                return False
            #float(fields[6])
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
    
######################################################################
    # Reading the csv and storing the values 
    # Reading transactions csv 
    lines_1 = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines_1=lines_1.filter(good_line_1)
    transactions_features=clean_lines_1.map(lambda l: (l.split(',')[6],(l.split(',')[7])))
    print(transactions_features.take(1))
    sum_by_key = transactions_features.reduceByKey(lambda a, b: float(a) + float(b))
    print(sum_by_key.take(10))
    
    # Reading the contracts csv
    lines_2 = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_lines_2=lines_2.filter(good_line_2)
    contracts_features=clean_lines_2.map(lambda l: (l.split(',')[0],1))
    print(contracts_features.take(1))

    
####################################################################################
    # Join on the address and to_address from the contracts and transactions values respectively
    tran_cont=contracts_features.join(sum_by_key)
    print(tran_cont.take(20))
    
    # Extracting the second element of the second element of the tuple
    second_elements = tran_cont.map(lambda x: float(x[1][1]))

    # Sorting the RDD by the second element of the tuple in descending order
    sorted_rdd = tran_cont.sortBy(lambda x: -float(x[1][1]))

    # Getting the top 10 tuples in the sorted RDD
    top_10_rdds = sorted_rdd.take(10)

###########################################################################
    # writing output to a file
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Question-2' + date_time + '/top10_contracts.txt')
    my_result_object.put(Body=json.dumps(top_10_rdds))

##############################################################################
    spark.stop()