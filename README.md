# Next Big Sound Data Engineer Challenge
This implementation downloads data sets from https://dumps.wikimedia.org/other/pagecounts-raw/2012/2012-01/.... and save it locally. It is then read into a data frame by Spark and transformed into "Top 10 most popular songs by language".


## Usage
```
usage: spark-submit batch.py [-h] [-l] -y YEAR -m MONTH -d DAY -H HOUR [-M MINUTE]
                [-s SECOND] [-D DEST]

optional arguments:
  -h, --help            show this help message and exit
  -l, --local           save data set to local file system. Otherwise, it will
                        be loaded to the user's home directory on HDFS.
                        Default is local
  -y YEAR, --year YEAR  zero padded hour
  -m MONTH, --month MONTH
                        zero padded month
  -d DAY, --day DAY     zero padded day
  -H HOUR, --hour HOUR  zero padded hour
  -M MINUTE, --minute MINUTE
                        zero padded minute
  -s SECOND, --second SECOND
                        zero padded second
  -D DEST, --dest DEST  output directory. default is results
```

There is a shell script that is easier to invoke which downloads the first hour of 2012.
```
$ ./run.sh
```

## Source
See [batch.py](batch.py). This python script contains both the ingestion, cleansing and analysis of the data.

## Ingestion
This implementation uses the input parameters defined in usage and saves the data set locally or on HDFS. Writing to HDFS is still a TODO. A database is not used here or required since Spark can load the data set into a data frame. For larger data sets or to consume multiple data set files, you will need to upload into HDFS for distributed processing with YARN and Spark 2. Also a TODO is the ability to consume multiple data set files at once.

## Data Preparation
The source contains comments that describes the preparation process before it's sent to analysis. In a production system and depending on the rate of ingestion, it would make sense to separate ingestion, preparation and analytics into separate processes. This will enable you to scale them differently.

### Steps 
* map - parses the row and converts column 3 (2 zero based) to a long type
* oDF - assigns column headers to the DF schema
* drop - drops the column since we don't need it and don't want to hold it in memory
* filter - filters out records using regex

## Why Spark 2
It has both stand-alone and distributed capabilities which makes it easier to develope an deploy to a cluster. It is also a fast, in-mem, distributed application framework that can process large amounts of data. Also Spark 2 is 10x faster than Spark 1.

## Compression and Data Storage Format
The data set was left in gzip format for convenience. GZIP compression uses more CPU resources than Snappy or LZO, but provides a higher compression ratio. GZip is often a good choice for cold data, which is accessed infrequently. Snappy or LZO are a better choice for hot data, which is accessed frequently. [Reference](https://www.cloudera.com/documentation/enterprise/5-15-x/topics/admin_data_compression_performance.html#xd_583c10bfdbd326ba-7dae4aa6-147c30d0933--7af7). As part of the ingestion process, the data set should be decompressed and using Snappy.

Since we consume most of the columns and there are not a lot of columns, the suggested format is Avro which is a row-based format. Parquet is a column-based format and would be preferable for data sets with lots of columns of which you're only consuming a few columns.

## Windowing
This implementation uses windowing in spark to partition the data by language and order them by the number of non_unique_views views descending. The window uses row_number() window to add a column containing the row number. This enables me to select only the 10 rows which corresponds to the top ten most popular wiki pages.

