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




