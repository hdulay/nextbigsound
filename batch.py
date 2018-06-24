#!/bin/python

import argparse
import os
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import *
from pyspark.sql.window import Window
import urllib2


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l','--local',dest='local',required=False,\
        help="save data set to local file system. Otherwise, it will be loaded to the \
                user's home directory on HDFS. Default is local", \
        default=True, \
        action='store_true')
    parser.add_argument('-y','--year',dest='year',required=True,help='zero padded hour', type=check_format)
    parser.add_argument('-m','--month',dest='month',required=True,help='zero padded month', type=check_format)
    parser.add_argument('-d','--day',dest='day',required=True,help='zero padded day', type=check_format)
    parser.add_argument('-H','--hour',dest='hour',required=True,help='zero padded hour', type=check_format)
    parser.add_argument('-M','--minute',dest='minute',required=False,help='zero padded minute', default="00", type=check_format)
    parser.add_argument('-s','--second',dest='second',required=False,help='zero padded second', default="00", type=check_format)
    parser.add_argument('-D','--dest',dest='dest',required=False,help='output directory. \
        default is results', default="results", type=str)
    args = parser.parse_args()

    file_name = download(args)

    spark = SparkSession.builder.\
        appName("pandora data engineering challenge"). \
        getOrCreate()

    # map - parses the row and converts column 3 (2 zero based) to a long type
    # toDF - assigns column headers to the DF schema
    # drop - drops the column since we don't need it and don't want to hold it in memory
    # filter - filters out records using regex
    pages = spark.read.text(file_name)\
        .rdd.map(lambda row: [ long(v) if i == 2 else v for  i, v in enumerate(row['value'].split(" "))])\
        .toDF(["language", "page_name", "non_unique_views", "bytes_transferred"])\
        .drop("bytes_transferred")\
        .filter(~col("page_name").rlike("(^[A-Za-z0-9]*:)"))

    top10bylang(args, pages)

    spark.stop()

# used to check the format of the arguments
def check_format(value):
    if value is not None and len(value) > 1:
        return value

    raise ValueError('value needs to be zero padded')

# download the file
def download(args):
    
    url = "https://dumps.wikimedia.org/other/pagecounts-raw/{0}/{0}-{1}/pagecounts-{0}{1}{2}-{3}{4}{5}.gz"\
        .format(args.year, args.month, args.day, args.hour, args.minute, args.second)

    file_name = "pagecounts-{0}{1}{2}-{3}{4}{5}.gz".format(args.year, args.month, args.day, args.hour, args.minute, args.second)

    if not os.path.exists(file_name):
        file = open(file_name, "w") 
        response = urllib2.urlopen(url)
        data = response.read()
        file.write(data)
        file.close()

    print "download complete"
    return file_name

def top10bylang(args, pages):
    # Defines partitioning specification and ordering specification.
    windowSpec = Window \
        .partitionBy(col("language")) \
        .orderBy(col("non_unique_views").desc())
    
    # filters only for the top 10 records
    result = pages.select("language", "page_name", "non_unique_views", row_number().over(windowSpec).alias("row")).where("row < 11")

    # format and repartition to create a single csv file
    result.drop("row").orderBy("language", col("non_unique_views").desc())\
        .repartition(1).write.mode("overwrite")\
        .csv("{}/{}".format(args.dest, "top10bylang"))


if __name__ == "__main__":
    main()
