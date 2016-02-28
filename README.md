This readme describes the business context (***Use Cases***), overall architecture and design choices (***Analytics Platform***) and outlines about the code (***Code Structure***).

# Use Cases

## Value proposition
Shoot2Top is building a platform to show live performances of videos posted on social networks,  report ratings on how video content performed once it was published and predict how effective different video virality strategies may play out. Our platform essentially serves three modules that are part of a suite of products that serve different specific needs but all share similar data architecture and resources.

## Product description
Shoot2Top continuously ingest videos published from major social networks like Twitter, Youtube and Facebook to analyze them real time to show media publishers how their videos are performing live. It also generates reports after-the-fact that highlights how each video and each media publisher performed. It also plans to use data collected to create analytical and machine learning tools to help predict optimal virality strategies.

Our suite of products include:
- Identifying when to post, which hashtags, and keywords to use for maximum virality
- Identifying key micro-trends in content that can enhance visibility
- Identifying opportunities to cross-promote your existing content based on current micro-trends


# Analytics Platform
The analytics platform for this project consist of three major components:
- Data Ingest Pipeline that can consume, store, and replay social network streams
- Data Lake for archiving streaming data for replay and repeat analysis
- Online Analytical Processing for mining features and generating insight

Our working hypothesis is: in order to maximize organic growth in eyeballs, shares, and followers, we had to act on micro-trends as they were happening. This meant our architecture had a near-real-time capability requirement, ruling out pure batch architectures.

## Architecture
Motivated by the needs for near-real-time processing, we considered where the bottlenecks in the system are likely to arise. The biggest limiting factor is data acquisition: acquiring targeted streaming data from social networks as it happens is efficient are cost-effective (or free), whereas getting access to a large historical data set is cost-prohibitive.

The natural data format of our application is, therefore, streams. To develop and maintain analytics, we needed to store and be able to act on data we previously collected as it was happening. However, we wanted to avoid the cost of implementing the analytical platform twice: once in batch and once more in near-real-time. Solving for these constraints, we settled on a variant of the Kappa Architecture. 

Production analytics happens in near-real-time, and algorithms development is enabled by the capability to store streaming data in the Data Lake, and the replay streams for development.

![Kappa Architecture Diagram](Kappa-Architecture.png)

The technology choices that enable this are:
- Ingest - a lightweight configuration-driven Python framework (scale-out)
- Data Lake - S3 bucket with partitioned Hive table-like organization
- Analytics - lightweight Python framework (scale-out capable)
- OLAP - Postgres as a Data Warehouse (scaling up to Amazon Redshift DW)
- End User - the end user application is an IPython notebook connected to the DW

## Data Ingest Pipeline
The Data Ingest Pipeline is responsible for streaming data from its primary sources (Twitter, Facebook, YouTube) or a replay source (S3, Local), and dispatching a stream to one or more streaming data sinks. The pipeline consists of several key components:
- The core Data Ingest Framework, which consumes a configuration file to instantiate at least one source and at least one sink, and wires them together by stream-chaining
- The Streamable Object Primitive - for which we chose a single social media post (tweet, post, or video, for Twitter, Facebook, and YouTube, respectively), represented as a self-describing JSON-equivalent Python object
  -    For example a tweet is: `{ “tweet”: { “created_at” : “...”, “text” : “Hello, World”, … } }`
  -    This enables the data to be context-free (since entity types are self-documenting and always contain timestamps) and the framework to be non-opinionated
- The Stream of posts, which is represented as a Python iterable of streamable objects
  -    Enables clear semantics for consuming the stream (for item in source:)
- The Sources, which each emit a stream - if the underlying data is batched, the sources are responsible for mapping the batches to streams. The sources are:
  -    Twitter - emits streams of tweetsbased upon topics of interest
  -    Facebook - emits streams of posts based upon topics of interest
  -    YouTube - emits streams of videos based upon topics of interest
  -    S3 - replays a stream captured to S3
  -    Local - replays a stream captures to the Local filesystem
- The Sinks, which consume a stream - if the underlying target is batched (e.g. S3), the sinks are responsible for batching writes appropriately
  -    Postgres - performs feature extraction and writes each post to Postgres/Redshift
  -    Socket - send the stream to a local socket for ingest into Apache Spark
  -    S3 - captures a stream to S3 for future repeat analysis
  -    Local - captures a stream to the Local filesystem for repeat analysis

The overall Data Ingest Pipeline component architecture is captured by the diagram below. 
![Data Ingest Pipeline](Ingest-Full.png)

As requirements evolve, this framework becomes a natural candidate for migration to Streamparse, as all the sinks and sources are already de-coupled, and the coordinating component maps cleanly to a topology.

##Data Lake

The data lake is stored in an S3 bucket. A structure similar to a partitioned Hive table in HDFS is used to ensure that multiple instances of the Data Ingest Pipeline can write to the Data Lake without coordination in advance and so that the data can be retrieved by partition.

Each instance of the S3 sink generates a unique folder name based on the timestamp the partition was created and a unique identifier. Inside each folder, the S3 sink generates sequential numbers for each micro-batch that it writes, similar to a MapReduce output. For example, three instances that all begin a new partition at midnight on December 18, 2015, will generate a structure like this:

```{text}
/w205-social-media-dec <- S3 bucket
  /2015-12-18T00:00:00-afa5ac3a-e695-4f96-aca2-d24c7527aed7
    0000000
    0000001
    0000002
    ...
  /2015-12-18T00:00:00-761df5cd-ca48-412f-a860-6d01022ff9b6
    0000000
    0000001
    0000002
    ...
  /2015-12-18T00:00:00-b0126517-8996-4e89-b0af-6a5eca83c404
    0000000
    0000001
    0000002
    ...
```

This structure (especially the unique identifier) enables an arbitrary number of instances of the Data Ingest Pipeline to operate in parallel without advance coordination, and for consumers to replay a consistent partition across all source instances, for example by evaluating the glob 2015-12-18T00:00:00-*. A side-channel partition index can be added if S3 list performance becomes a limiting consideration.

Local data for replay is stored in an equivalent structure in the file system, enabling sampling for development by downloading select files using standard tools (for example, s3cmd).

Each file in the Data Lake contains a single micro-batch of posts, organized as a JSON array for easy manipulation with standard tools. For example, a micro-batch from Twitter might looks as follows (white-space added for clarity).
```{text}
[
  { “tweet”: { “created_at” : “2015-12-18T00:00:01”, “text” : “See you later, alligator!”, … } },
  { “tweet”: { “created_at” : “2015-12-18T00:00:02”, “text” : “In a while, crocodile!”, … } },
  …
]
```

# Code Structure

The core entry point is `DataIngest.py`. Data Ingest expects a configuration file as an argument that defines and configures the sink and source for the instance (this construct keeps the code separate from the data, especially sensitive credentials). The syntax to run it is:

```sh
python DataIngest.py --config /path/to/config.file.cfg
```

Where the configuration file is at `/path/to/config.file.cfg` on the local filesystem.

The Data Ingest script relies on a number of data source implementations represented as Python classes:
- `TwitterDataIngestSource.py` - Source for ingesting data from the Twitter 1.1 streaming API
- `FacebookDataIngestSource.py` - Source for ingesting data from the Facebook 2.5 REST API
- `YouTubeDataIngestSource.py` - Source for ingesting data from the Youtube v3 REST API
- `S3DataIngestSource.py` - Source for ingesting data from S3 (typically, in a replay of a ingest from one or more of the above)
- `LocalDataIngestSource.py` - Source for ingesting data from the local filesystem (typically, in a replay of a ingest from one or more of the above)

Sinks are duck-typed with the expectation that they expose a stream of data as a Python iterable. In other words:
```{python}
for item in source:
  do_something_with(item)
```
The Data Ingest script relies on a number of data sink implementations as Python classes:
- `S3DataIngestSink.py` - Save the data to S3
- `LocalDataIngestSink.py` - Save the data to the Local filesystem
- `PostgresDataIngetsSink.py` - Extract key features and write them to the Postgres-comptable DW for OLAP processing
- `SocketDataIngest.py` - Send data to a socket (for ingest by Apache Spark)

The code also includes two examples of configuration files (without credentials):
- `.w205-data-ingest.cfg` - Ingest data from Twitter to S3
- `.w205-local-ingest.cfg` - Ingest data from Twitter into the local filesystem
- `.w205-postgres-sample.cfg` - Ingest a data sample stored locally into Postgres
