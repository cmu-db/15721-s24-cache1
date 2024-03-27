### Features
1. Add in-memory cache for small and hot data.
2. Integrate RocksDB.
3. Implement client and its communicaiton with the server.
### Optimization
1. Discuss the key for the cache. Currently it is raw S3 request string, but maybe we can make it as `bucket + one key`, since if one key represents a file, then for different requests, keys may overlap, and the current implementation will waste disk space.
2. Solve the read-write & write-write conflict for disk operations. Let's say we have 2 requests, and they have same key for the cache, the first request has a cache miss, then it has to go to S3 to fetch data. But now second request comes, and the first request hasn't pulled all the data from S3 and stored them in the disk. So the disk file exists, but the content is not complete. Here is one possible way: when a cache miss happens, we record this key into our cache structure, but with a status `uncompleted`, and when the data is fully pulled and stored, the status is updated to `completed`. When another same request comes and tries to `get` disk file from cache, our cache finds the key exists but its status is `uncompleted`. So the cache should wait until its status turns to `completed`. (The async design to make the cache not simply dry wait is worth discussing)
3. In async approach, we should support read & output data at the same time, both S3 read & disk write(or network write) and disk read & network write. To support this, we cannot use one single fixed buffer, which is the current implementation of disk_manager. Here is 2 ways to support this: 
   1. a buffer consumed and extended(see `s3.rs`); 
   2. two fixed buffers. 
   
    (Apart from it, do we have another method to make async version gain more performance than `sync` with `multiple threads`?)
4. Discuss other possibilities to parallel.
5. Design benchmark and add statistics for benchmark.
6. For stream and iterator, buffer_size is one thing we should consider. If it is too small, then we have to do more frequent I/Os, but if it is too big, then we cannot serve too many requests at the same time due to fixed total memory size. Maybe we can adjust the buffer_size dynamically.