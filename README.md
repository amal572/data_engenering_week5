# Introduction to Batch Processing

## DE Zoomcamp 5.1.1 - Batch vs Streaming

Batch processing: processing chunks of data at regular intervals.
Example: processing taxi trips each month.
A batch job can be scheduled in many ways: (Weekly, Daily, Hourly)

Streaming: processing data on the fly.
Example: processing a taxi trip as soon as it's generated.

#### Pros and cons of batch jobs
<li>Advantages:</li>
Easy to manage. There are multiple tools to manage them (the technologies we already mentioned)
Re-executable. Jobs can be easily retried if they fail.
Scalable. Scripts can be executed in more capable machines; Spark can be run in bigger clusters, etc.
<li>Disadvantages:</li>
Delay. Each task of the workflow in the previous section may take a few minutes; assuming the whole workflow takes 20 minutes, we would need to wait those 20 minutes until the data is ready for work.

## Introduction to Spark
Spark is an open-source, distributed computation engine that:

<li> Distributes workloads: Splits data across multiple machines/nodes, enabling parallel processing and faster analysis.</li>
<li> Handles diverse data: Works with various data formats, from structured databases to unstructured text files.</li>
<li> Offers multiple languages: Can be programmed with Scala, Java, Python, R, or SQL.</li>
Supports multiple use cases: Tackle batch processing, real-time streaming, machine learning, and more.
In this module, we will be using PySpark, a Python API for Apache Spark. It enables you to perform real-time, large-scale data processing in a distributed environment using Python. PySpark supports all of Spark’s features such as Spark SQL, DataFrames, Structured Streaming, Machine Learning (MLlib), and Spark Core.
The wrapper for Python is called PySpark.

Spark can deal with both batches and streaming data. The technique for streaming data is seeing a stream of data as a sequence of small batches and then applying similar techniques on them to those used on regular badges. We will cover streaming in detail in the next lesson.

## Installing Spark for Windows

first, install the Java Development Kit (JDK): Apache Spark requires Java. Make sure you have Java Development Kit (JDK) installed on your system. You can download and install it from the official Oracle website:
```bash
https://www.oracle.com/java/technologies/downloads/
```
second, Download Apache Spark:
Go to the Apache Spark website: Apache Spark Downloads.
```bash
https://spark.apache.org/downloads.html
```
Select the latest stable version of Spark and download the pre-built version for Hadoop.
Choose a version that matches your Hadoop version. If you don't have Hadoop installed, you can choose the version of the latest Hadoop (currently Hadoop 3.2).

then, Extract Spark Archive:
Extract the downloaded Spark archive to a directory on your system. For example, you can extract it to C:\spark.

Set Environment Variables:
Add SPARK_HOME to your system environment variables and set its value to the directory where you extracted Spark—for example, C:\spark.
Add %SPARK_HOME%\bin to the PATH environment variable.

Configure Hadoop:
If you're using Spark with Hadoop, you may also need to configure Hadoop-related environment variables. Set HADOOP_HOME to the directory where Hadoop is installed, and add %HADOOP_HOME%\bin to the PATH.

install findspark: pip install findspark

then import it into your Jupiter notebook it helps you to find Pyspark: 
```bash
import findspark
findspark.init()
```
note: some issues in Windows to run Spark UI in port 4040 were solved by downloading winutils.exe for Hadoop and importing it in the bin folder in spark

## Creating a Spark session
We can use Spark with Python code using PySpark. We will be using Jupyter Notebooks for this lesson.

We first need to import PySpark to our code:
```bash
import pyspark
from pyspark.sql import SparkSession
```
We need to instantiate a Spark session, an object used to interact with Spark.
```bash
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```
* SparkSession: is the class of the object that we instantiate. builder is the builder method.
* master() sets the Spark master URL to connect to. The local string means that Spark will run on a local cluster. [*] means that Spark will run with as many CPU cores as possible.
* appName() defines the name of our application/session. This will show in the Spark UI.
* getOrCreate() will create the session or recover the object if it was previously created.
  
Once we've instantiated a session, we can access the Spark UI by browsing to localhost:4040. The UI will display all current jobs.

## Reading CSV files
Similar to Pandas, Spark can read CSV files into dataframes, a tabular data structure. Unlike Pandas, Spark can handle much bigger datasets but infer the datatypes of each column.
Let's read the file and create a dataframe:
```bash
df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-01.csv')
```
* read() reads the file.
* option() contains options for the read method. In this case, we're specifying that the first line of the CSV file contains the column names.
* csv() is for reading CSV files.
You can see the contents of the dataframe with df.show() (only a few rows will be shown) or df.head(). You can also check the current schema with df.schema; you will notice that all values are strings.

We can use a trick with Pandas to infer the datatypes:

1. Create a smaller CSV file with the first 1000 records.
2. Import Pandas and create a Pandas dataframe. This dataframe will have inferred datatypes.
3. Create a Spark dataframe from the Pandas dataframe and check its schema
```bash
spark.createDataFrame(my_pandas_dataframe).schema
```
4. Based on the output of the previous method, import types from pyspark.sql and create a StructType containing a list of the datatypes.
```bash
from pyspark.sql import types
schema = types.StructType([...])
```
5. Create a new Spark dataframe and include the schema as an option.
```bash
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-01.csv')
```


## Partitions

In PySpark, partitions refer to the division of a dataset into smaller, more manageable chunks for processing. When you load data into a PySpark DataFrame or RDD (Resilient Distributed Dataset), it is divided into partitions, with each partition representing a subset of the data. These partitions are distributed across the nodes in a Spark cluster.

```bash
# create 24 partitions in our dataframe
df = df.repartition(24)
# parquetize and write to fhvhv/2021/01/ folder
df.write.parquet('fhvhv/2021/01/')
```

Trying to write the files again will output an error because Spark will not write to a non-empty folder. You can force an overwrite with the mode argument:
```bash
df.write.parquet('fhvhv/2021/01/', mode='overwrite')
```

## Spark DataFrame

DataFrame operations in Spark fall into two categories: transformations and actions.

- **Transformations:** These are lazily evaluated operations that create a new DataFrame from an existing one without triggering any Spark jobs. Examples include `select()`, `filter()`, `groupBy()`, and `orderBy()`.

- **Actions:** Actions are eager operations that trigger the execution of previously defined transformations and return a result to the driver program. Examples include `show()`, `count()`, `collect()`, and `write()`.

## Common Operations

### Transformations

- `select()`: Selects a subset of columns from the DataFrame.
- `filter()`: Filters rows based on a given condition.
- `groupBy()`: Groups the DataFrame rows by a given column.
- `orderBy()`: Sorts the DataFrame based on one or more columns.
- `withColumn()`: Adds a new column to the DataFrame or replaces an existing one.
- `drop()`: Drops specified columns from the DataFrame.
- `join()`: Performs a join operation between two DataFrames.
- `groupBy().agg()`: Performs aggregation operations after grouping.
- `distinct()`: Returns distinct rows from the DataFrame.

### Actions

- `show()`: Displays the content of the DataFrame in a tabular format.
- `count()`: Returns the number of rows in the DataFrame.
- `collect()`: Retrieves all data from the DataFrame to the driver program.
- `write()`: Writes the DataFrame to an external storage system.
- `saveAsTable()`: Saves the DataFrame as a table in a database.
- `take()`: Returns the first `n` rows from the DataFrame.

## Functions and User-Defined Functions (UDFs)
#### Functions:
Built-in Functions: PySpark provides a wide range of built-in functions as part of its API. These functions are already implemented and optimized within the framework.

Purpose: Built-in functions in PySpark are designed to perform common data processing tasks such as filtering, aggregation, sorting, and more. They are optimized for performance and scalability.

Usage: Built-in functions can be directly used within DataFrame or RDD operations without requiring additional definition or registration. They are accessed through the PySpark API.

#### User-Defined Functions (UDFs):
Custom Functions: UDFs in PySpark are custom-defined functions created by users to extend the functionality of the framework beyond what is provided by built-in functions.

Purpose: UDFs are used to apply custom logic or business rules to manipulate data within DataFrame operations. They allow users to perform specialized transformations or computations.

Usage: UDFs need to be explicitly defined and registered with PySpark before they can be used. Once registered, they can be applied to DataFrame columns using the withColumn() method or within SQL queries using the registerFunction() method.

#### Example:
```bash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("UDF Example") \
    .getOrCreate()

# Sample DataFrame
df = spark.createDataFrame([(1, "John"), (2, "Alice"), (3, "Bob")], ["id", "name"])

# Built-in function example
df_filtered = df.filter(col("id") > 1)

# User Defined Function (UDF) example
def length_udf(name):
    return len(name)

length_udf = udf(length_udf, IntegerType())
df_with_length = df.withColumn("name_length", length_udf(col("name")))

df_filtered.show()
df_with_length.show()
```
## GROUP BY in Spark
Let's do the following query:
```bash
df_green_revenue = spark.sql("""
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2  
""")
```
## Joins in Spark
Joining tables in Spark is implemented in a similar way to GROUP BY and ORDER BY, but there are 2 distinct cases: joining 2 large tables and joining a large table and a small table.

### Joining 2 large tables
Let's assume that we've created a df_yellow_revenue dataframe in the same manner as the df_green_revenue we created in the previous section. We want to join both tables, so we will create temporary dataframes with changed column names so that we can tell apart data from each original table:
```bash
df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')
```
We will now perform an outer join so that we can display the amount of trips and revenue per hour per zone for green and yellow taxis at the same time regardless of whether the hour/zone combo had one type of taxi trips or the other:
```bash
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
```
on= receives a list of columns by which we will join the tables. This will result in a primary composite key for the resulting table.
how= specifies the type of JOIN to execute.
When we run either show() or write() on this query, Spark will have to create both the temporary dataframes and the joint final dataframe

### Joining a large table and a small table
Let's now use the zones lookup table to match each zone ID to its corresponding name.
```bash
df_zones = spark.read.parquet('zones/')

df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)

df_result.drop('LocationID', 'zone').write.parquet('tmp/revenue-zones')
```
he default join type in Spark SQL is the inner join.
Because we renamed the LocationID in the joint table to zone, we can't simply specify the columns to join and we need to provide a condition as criteria.
We use the drop() method to get rid of the extra columns we don't need anymore, because we only want to keep the zone names and both LocationID and zone are duplicate columns with numeral ID's only.
We also use write() instead of show() because show() might not process all of the data.

## Resilient Distributed Datasets (RDDs)
RDD is a distributed collection of elements that can be operated on in parallel. It is the following properties

Resilient - Fault-tolerant with the help of RDD lineage graph [DAG] and so able to recompute missing or damaged partitions due to node failures.
Lazy evaluated - Data inside RDD is not available or transformed until an action triggers the execution.
Cacheablec - All the data can be hold in a persistent “storage” like memory (default and the most preferred) or disk (the least preferred due to access speed).
Immutable or Read-Only - It does not change once created and can only be transformed using transformations to new RDDs.

### From Dataframe to RDD
Resilient Distributed Datasets (RDDs) are a fundamental data structure in Apache Spark that represent immutable, distributed collections of objects.

implement how to convert from dataframe to RDD
```bash
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
```
We can re-implement the SELECT section by choosing the 3 fields from the RDD's rows.
```bash
rdd = df_green \
    .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
    .rdd
```
We can implement the WHERE section by using the filter() and take() methods:

filter() returns a new RDD cointaining only the elements that satisfy a predicate, which in our case is a function that we pass as a parameter.
take() takes as many elements from the RDD as stated.
```bash
from datetime import datetime

start = datetime(year=2020, month=1, day=1)

def filter_outliers(row):
    return row.lpep_pickup_datetime >= start

rdd.filter(filter_outliers).take(1)
```

## Spark and Docker
we can run Sparks in the docker after downloading Spark and adding it to the control panel then in the docker file we can get the:
```bash
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${SPARK_HOME}/python:${PYTHONPATH}"
ENV PATH="${HOME}/.local/bin:${PATH}"
```
then we have to make a docker-compose file to run the service (spark, spark-worker, jupyter):
```bash
version: '2'

services:
  spark:
    #image: docker.io/bitnami/spark:3.3
    build: 
      context: .
      dockerfile: Dockerfile    
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - '8080:8080'
      - '4040:4040' # Spark master UI

  spark-worker:
    image: docker.io/bitnami/spark:3.3
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - '8081:8081' # Spark worker UI

  jupyter:
    image: jupyter/pyspark-notebook
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - PYSPARK_PYTHON=python3
      - PYSPARK_DRIVER_PYTHON=python3
      - SPARK_MASTER=spark://spark:7077
    volumes:
      - "./:/home/jovyan/work"
    ports:
      - '8888:8888' # Jupyter Notebook
    depends_on:
      - spark

```
make sure that your version is the same as Spark version 3.3 or change it to your version 
you can run jupyter in port 8888
