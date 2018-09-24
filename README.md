# test
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
See [batch.py](batch.py). This python script contains both the ingestion, cleansing and analysis of the data. In production, it might be best to separate these 3 steps into their own applications that can be scaled individually. 

## Ingestion
This implementation uses the input parameters defined in usage and saves the data set locally or on HDFS. **Writing to HDFS is still a ___TODO___**. A database is not used nor is it required since Spark can load the data set file into a data frame. For larger data sets, I recommend uploading into HDFS for distributed processing with YARN and Spark 2. Also a ***TODO*** is the ability to download multiple data set files and process them together.

## Data Preparation
The source code contains comments that describes the preparation process before it's sent to analysis. In a production system and depending on the rate of ingestion, it would make sense to separate ingestion, preparation and analytics into separate processes. Most likely preparation of data will take longer and we don't want that affecting ingestion rate.

### Steps 
* map - parses the row and converts column 3 (2 zero based) to a long type
* toDF - assigns column headers to the DF schema
* drop - drops the column since we don't need it and don't want to hold it in memory
* filter - filters out records using regex

## Why Spark 2
It has both stand-alone and distributed capabilities which makes it easier to develope an deploy to a cluster. It is also a fast, in-mem, distributed application framework that can process large amounts of data. Also Spark 2 is 10x faster than Spark 1.

## Compression and Data Storage Format
The data set was left in gzip format for convenience. GZIP compression uses more CPU resources than Snappy or LZO, but provides a higher compression ratio. GZip is often a good choice for cold data, which is accessed infrequently. Snappy or LZO are a better choice for hot data, which is accessed frequently. [Reference](https://www.cloudera.com/documentation/enterprise/5-15-x/topics/admin_data_compression_performance.html#xd_583c10bfdbd326ba-7dae4aa6-147c30d0933--7af7). As part of the ingestion or preparation steps, the compression algorithm can be changed to Snappy.

Since we consume most of the columns and there are not a lot of columns, the suggested format is Avro which is a row-based format. Parquet is a column-based format and would be preferable for data sets with lots of columns of which we're only consuming a few.

## Windowing
This implementation uses windowing in spark to partition the data by language and order them by the number of non_unique_views views descending. The window uses ***row_number()*** window to add a column containing the row number within a partition. This enables us to select only the 10 rows which corresponds to the top ten most popular wiki pages.

## Saving to CSV
The results are written into a single CSV file. To accomplish this, the data has to be either ***repartition(1)*** or ***coalesce(1)*** . Repartitioning will redistribute the data across executors for balanced parallelism. Coalesce does not redistribute data and could cause a single executor to process most of the data.

## Pipeline Stages

### After Data Preparation
```
>>> pages.show()
+--------+--------------------+----------------+
|language|           page_name|non_unique_views|
+--------+--------------------+----------------+
|    aa.d|           Main_Page|               1|
|      aa|%D0%92%D0%B0%D1%8...|               1|
|      aa|Meta.wikimedia.or...|               1|
|      aa|meta.wikimedia.or...|               1|
|    ab.d|%D0%90%D0%BC%D0%B...|               1|
|    ab.d|%D0%90%D0%BC%D0%B...|               1|
|    ab.d|%D0%90%D1%88%D0%B...|               1|
|    ab.d|%D0%90%D1%88%D0%B...|               1|
|    ab.d|%D0%97%D0%B0%D0%B...|               1|
|    ab.d|%D0%B7%D0%B0%D0%B...|               1|
|    ab.d|          windshield|               1|
|   ab.mw|                  ab|               9|
|    ab.q|     Help%3AContents|               1|
|    ab.q|          Help%3AFAQ|               1|
|    ab.q|       Help%3AManual|               1|
|    ab.q|                  Wq|               1|
|      ab|%2525D0%252590%25...|               1|
|      ab|%D0%90%D0%B2%D0%B...|               1|
|      ab|%D0%90%D0%B2%D0%B...|               1|
|      ab|%D0%90%D0%B2%D0%B...|               1|
+--------+--------------------+----------------+
only showing top 20 rows

```

### After Windowing
```
>>> result.show()
+--------+--------------------+----------------+---+
|language|           page_name|non_unique_views|row|
+--------+--------------------+----------------+---+
| cbk-zam|   El_Primero_Pagina|               4|  1|
| cbk-zam|                1868|               2|  2|
| cbk-zam|                1939|               2|  3|
| cbk-zam|          1_de_Enero|               2|  4|
| cbk-zam|                2010|               2|  5|
| cbk-zam|       20_de_Febrero|               2|  6|
| cbk-zam|         21_de_Enero|               2|  7|
| cbk-zam|       27_de_octubre|               2|  8|
| cbk-zam|     29_de_Diciembre|               2|  9|
| cbk-zam|     30_de_Diciembre|               2| 10|
|    ro.b|%C5%9Eah/Reguli_d...|               5|  1|
|    ro.b|Englez%C4%83/Cuvi...|               2|  2|
|    ro.b|Karaoke_-_versuri...|               2|  3|
|    ro.b|Manual_de_impleme...|               2|  4|
|    ro.b|Medicin%C4%83/Ulc...|               2|  5|
|    ro.b|     %C5%9Eah/Regele|               1|  6|
|    ro.b|%E0%A4%B5%E0%A4%B...|               1|  7|
|    ro.b|Artroplastia_tota...|               1|  8|
|    ro.b|Carte_de_bucate:S...|               1|  9|
|    ro.b|Englez%C4%83/Gram...|               1| 10|
+--------+--------------------+----------------+---+
only showing top 20 rows
```

### Saved data
```
>>> result.drop("row").orderBy("language", col("non_unique_views").desc()).show()
+--------+--------------------+----------------+
|language|           page_name|non_unique_views|
+--------+--------------------+----------------+
|      aa|meta.wikimedia.or...|               1|
|      aa|Meta.wikimedia.or...|               1|
|      aa|%D0%92%D0%B0%D1%8...|               1|
|    aa.d|           Main_Page|               1|
|      ab|%D0%98%D1%85%D0%B...|               5|
|      ab|%D0%98%D0%B0%D0%B...|               3|
|      ab|%D0%90%D1%82%D3%9...|               2|
|      ab|%D0%90%D2%A7%D1%8...|               2|
|      ab|%D0%90%D0%BB%D0%B...|               2|
|      ab|%D0%90%D0%BA%D0%B...|               2|
|      ab|%D0%90%D1%84%D0%B...|               2|
|      ab|%D0%90%D1%88%D0%B...|               2|
|      ab|%D0%90%D1%88%D0%B...|               2|
|      ab|%D0%90%D0%BC%D0%B...|               2|
|    ab.d|%D0%90%D0%BC%D0%B...|               1|
|    ab.d|%D0%90%D1%88%D0%B...|               1|
|    ab.d|%D0%97%D0%B0%D0%B...|               1|
|    ab.d|%D0%B7%D0%B0%D0%B...|               1|
|    ab.d|%D0%90%D0%BC%D0%B...|               1|
|    ab.d|          windshield|               1|
+--------+--------------------+----------------+
only showing top 20 rows
```
