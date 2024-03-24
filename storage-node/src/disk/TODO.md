1. benchmark read_to_end & read
2. benchmark vector & bytes
3. explore BufReader
4. when read batch, benchmark read & read_buf
5. explore write_all_buf, buf represents the S3?
6. explore write_vectored
7. when create, benchmark openoptions and directly create
8. write_disk input parameter &[u8] and other types (benchmark)

1. 研究stream，看cacache content，怎么实现batch? tykio的write方法？
2. 一个path会对应一个pub struct AsyncWriter(Mutex<State>); 里面存了path？
3. 用tmpfile改测试
4. cacache 靠最后写commit确保读写一致，似乎我们只有读写冲突，没有写写，cache记录一个写完和没写完？
5. cacache 是按commit来确保路径是最后commit的内容，其实每个write都会存下来，但是是不同文件，实际上是错开写