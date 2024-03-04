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
