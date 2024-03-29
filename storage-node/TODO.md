### Features
1. Add in-memory cache for small and hot data.
   For in-memory cache, currently we can have 2 designs of the whole procedure for write data into cache:
   Generally, memory cache will record a hashmap (key -> vector of Bytes) to record the data. (Note: in current version, the file represents all the files for one S3 request, and we should optimize it later.)
   1. First Design:
      1. Get the file size and file from S3.
      2. Storage_manager will check the file size, if it is small, try to put it into memory cache, (remember also write the evicted memory key to disk if applicable); otherwise, try to write it into disk cache.
   
      (Disk cache and memory cache could consume a `S3Reader` as an input parameter for write_data in this case)
   2. Second Dsign:
      1. Get the file from S3 (as S3Reader).
      2. Storage_manager will convert S3Reader to stream, and call next to get `Bytes`. Then, it will first try to write it to memory cache. 
   
            Memory cache will first add the key into its hashmap, and record the `Bytes`. But when it finds the size is too large, it will return false and also return the vector of `Bytes` for this key.

            Storage_manager will write the file with `false` return value from memory cache `or` the evicted file to disk.

      (Disk cache and memory cache should accept `Bytes` as an input parameter for write_data in this case)
   
   For me, the first design is very clear, and the second design can save the cost of requesting S3 to get the size. But I think the cost is very low. Maybe the major difference is the API for write_data?
2. Integrate RocksDB.
3. Implement client and its communicaiton with the server.
4. Pin & unpin to prevent eviction the disk file that is using. (Although this situation seems to rarely happen)
### Optimization
1. For cache policy like `lru.rs` and `lru_k.rs`, we can use fine-grained locks instead of one big lock to lock it. If we use fine-grained locks (at least after getting the value from cache, we don't need to hold its lock), we should add unpin/pin in `lru.rs` and `lru_k.rs`.
2. Discuss the key for the cache. Currently it is raw S3 request string, but maybe we can make it as `bucket + one key`, since if one key represents a file, then for different requests, keys may overlap, and the current implementation will waste disk space.
3. Solve the read-write & write-write conflict for disk operations. Let's say we have 2 requests, and they have same key for the cache, the first request has a cache miss, then it has to go to S3 to fetch data. But now second request comes, and the first request hasn't pulled all the data from S3 and stored them in the disk. So the disk file exists, but the content is not complete. Here is one possible way: when a cache miss happens, we record this key into our cache structure, but with a status `uncompleted`, and when the data is fully pulled and stored, the status is updated to `completed`. When another same request comes and tries to `get` disk file from cache, our cache finds the key exists but its status is `uncompleted`. So the cache should wait until its status turns to `completed`. (The async design to make the cache not simply dry wait is worth discussing)
4. In async approach, we should support read & output data at the same time, both S3 read & disk write(or network write) and disk read & network write. To support this, we cannot use one single fixed buffer, which is the current implementation of disk_manager. Here is 2 ways to support this: 
   1. a buffer consumed and extended(see `s3.rs`); 
   2. two fixed buffers. 
   
    (Apart from it, do we have another method to make async version gain more performance than `sync` with `multiple threads`?)
5. Discuss other possibilities to parallel.
6. Design benchmark and add statistics for benchmark.
7. For stream and iterator, buffer_size is one thing we should consider. If it is too small, then we have to do more frequent I/Os, but if it is too big, then we cannot serve too many requests at the same time due to fixed total memory size. Maybe we can adjust the buffer_size dynamically.
