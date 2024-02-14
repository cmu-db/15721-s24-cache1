# I/O Service Project Proposal

* Yuanxin Cao (yuanxinc)
* Lan Lou (lanlou)
* Kunle Li (kunlel)

## Overview

> What is the goal of this project? What will this component achieve?

The objective of this project is to develop an Input/Output (I/O) service for an Online Analytical Processing (OLAP) database system. This service will facilitate communication between the execution engine and remote storage solutions such as Amazon S3 or Microsoft Azure Blob. Additionally, a local cache will be incorporated to store recently accessed data on the local disk, thereby accelerating future data retrievals.

The I/O service is designed to manage requests from the execution engine, fetching pertinent data (e.g., Parquet files) from either the local cache or remote storage. It will process the data and return a stream of decoded information to the execution engine.

The initial phase aims to construct a fully functional I/O service following the specifications outlined above. Further enhancements, such as kernel bypass and integration of io_uring, may be considered based on project timeline and requirements.


## Architectural Design

> Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

The I/O service receives input in the form of requested columns (i.e. logical location) from the execution engine and produces an output stream (e.g. [`tokio::Stream`](https://docs.rs/tokio/latest/tokio/stream/index.html)) of Apache Arrow [`RecordBatch`](https://docs.rs/arrow-array/50.0.0/arrow_array/struct.RecordBatch.html).

![](./figs/io-arch.png)


Our design comprises several key components:

- Storage Client
- Storage Node
    - Storage Manager
    - LruCache
    - Storage Reader

The Storage Client resides in the compute node, where it establishes connections with the executors from the execution engine. The Storage Manager orchestrates requests from the compute node and then directs them to either the LruCache or the Storage Reader. The LruCache works exactly as implied by its name, recording the access timestamp and make evictions of the cached elements. The Storage Reader includes several APIs for reading from different storage systems such as Amazon S3, Microsoft Azure Blob, and the local file system.

The workflow of the I/O service is shown as follows: initially, the execution engine invokes the API exposed by the I/O service. The Storage Client will then contact the catalog to retrieve the corresponding physical location based on the logical columns (TBD). Next, the Storage Client transmits the requests using protocols such as gRPC (TBD) to the Storage Node. The Storage Manager then verifies whether the data is already present on the local disk by consulting the LruCache. This cache includes a mapping where the key represents the file's physical location in S3, and the value denotes the physical location on the local disk. If the data is found, it is directly returned to the Storage Client. Otherwise, the Storage Reader reads the data from S3 and updates the cache. Finally, the Parquet file is decoded in the Storage Client, and the resulting stream is returned to the execution engine.


## Design Rationale

> Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

The design goal of the I/O service is to provide the execution engine with a simple interface to interact with the storage while achieving high performance. The storage client resides in the compute node, which makes it possible to let the execution engine get storage data just by a function call, instead of sending a request over the network. This leaves request processing to the I/O service itself and thus makes the conveying of data or error more straightforward. Moreover, having a storage client residing on the compute node promises more possibilities, including providing a `write_data` interface for the execution engine to store its own persistent states (if there would be any) in the future. Also, the storage client could be extended to include a local cache for the compute node, which boost the performance a lot on cache hit by saving a network request to the storage node for data.

The storage node, on the other hand, is designed to be of high performance and at the same time be easily extendible. LRU is one of the most adopted cache strategies in the industry since it maintains a good hit rate in real-world scenarios while requiring moderate computation. The storage reader is designed to retrieve data from different storage services. Currently, we plan to support the local file systems and Amazon S3, but we can easily add more storage services in the future via the abstraction of the storage reader.

## Testing Plan

> How should the component be tested?

1. Correctness Tests

   1. Unit Tests

      1. Cache Algorithm Test: focusing on the correctness of cache algorithms, like LRU.
      2. Storage Reader Test: focusing on the correctness of getting data from the underlying storage.
      3. Storage Manager Test: focusing on the correctness of coordinating the cache and the storage reader.
      4. Storage Client Test: focusing on the correctness of getting physical location information from the catalog and forwarding the request to the I/O server.

      (Note: all the above tests should also focus on error handling.)

   2. Integration Tests 

      The integration test will use the public API of the I/O service. We will call the API of the storage client in the way execution engine does. We will test on different request types (table, column, etc.), different storage types (file system, S3), and different request data amount. We will focus on the availability of the data and the correctness of the contents. Also, the I/O service should report the error appropriately when there is an exception.

2. Performance Tests (Benchmark)

   We can test our component using a combination of diverse real-world datasets that have various data types, including numerical, string, and date/time data. These datasets should exhibit different characteristics such as NDV (Number of Distinct Values) ratio, null ratio, sortedness, and Zipf distribution. Some examples of datasets suitable for testing include UCI-ML, Yelp, IMDb, and the Public BI Benchmark. We can also generate our own data for testing.

   To simulate the behavior of AWS S3, we can mock it using a simple local server with a deliberately introduced delayed response time. Also, the benchmark tests should include a comparison of response times under various data request scenarios like sequential scan, index scan, etc.

## Trade-offs and Potential Problems

> Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

The whole design is based on the fact that the database is a static one (i.e. no data manipulation) and we only have read requests on the storage. This assumption makes everything easier, since there will be few concurrency issues for a read-only database. However, if we are going to enable updates, then we should correctly handle the read-write and write-write conflicts, which requires a more complicated design than the current one.

Moreover, even if all data are ETLed into the database system (this is our assumption), there can still be updates if the user replaces some of the underlying Parquet files. In this case, we might need another service to perform data discovery on the storage to deal with these situations. Also, we have to ensure the consistency of caches in different compute nodes (if we are going to build the cache) and ensure that the data we read is not stale.


<!-- ## Glossary (Optional)
> If you are introducing new concepts or giving unintuitive names to components, write them down here. -->

