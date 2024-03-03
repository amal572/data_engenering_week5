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


