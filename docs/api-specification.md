# I/O Service API Specification

## Overview

> What commands will the API expose.

The I/O service will provide the execution engine with a client library, to which they can issue requests for data. We allow the execution engine to query data on different granularities, including table, column, and tuple. We will provide both synchronous and asynchronous methods for the execution engine to get storage data. See https://github.com/cmu-db/15721-s24-cache1/pull/2 for more details.

## Encoding

> What encoding scheme will the API use for inputs / outputs

We will encode the data as [Arrow's `RecordBatch` type](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) when we transfer the storage data to the execution engine.

## Error Handling

> What errors can the service encounter and how will API handle them (e.g., status codes).

TODO