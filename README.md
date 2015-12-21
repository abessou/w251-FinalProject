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

The technology choices that enable this were:
- Ingest - a lightweight configuration-driven Python framework (scale-out)
- Data Lake - S3 bucket with partitioned Hive table-like organization
- Analytics - lightweight Python framework (scale-out capable)
- OLAP - Postgres as a Data Warehouse (scaling up to Amazon Redshift DW)
- End User - the end user application is an IPython notebook connected to the DW

## Data Ingest Pipeline
The Data Ingest Pipeline is responsible for streaming data from its primary sources (Twitter, Facebook, YouTube) or a replay source (S3, Local), and dispatching a stream to one or more streaming data sinks. The pipeline consists of several key components:
- The core Data Ingest Framework, which consumes a configuration file to instantiate at least one source and at least one sink, and wires them together by stream-chaining
- The Streamable Object Primitive - for which we chose a single social media post (tweet, post, or video, for Twitter, Facebook, and YouTube, respectively), represented as a self-describing JSON-equivalent Python object
  -    For example a tweet is: { “tweet”: { “created_at” : “...”, “text” : “Hello, World”, … } }
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
[
  { “tweet”: { “created_at” : “2015-12-18T00:00:01”, “text” : “See you later, alligator!”, … } },
  { “tweet”: { “created_at” : “2015-12-18T00:00:02”, “text” : “In a while, crocodile!”, … } },
  …
]

