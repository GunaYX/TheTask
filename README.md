# Calculate how much money each user has spent efficiently

- In a machine with 8 CPU cores, 4GB MEM, and 4TB HDD disk, there are two files:
  - A 1TB file with (item_id: uint64, item_price: uint64), which contains a lot of records about the price of each item.
  - A 1TB file with (user_id: uint64, item_id: uint64), which contains a lot of records about the items each user purchased.

- The files are unordered.

- Each item has an unique item_id.

- Each user has an unique user_id.
- Each user can purchase one or more different or the same items.

Please write a program to calculate how much money each user has spent efficiently and output the result to the disk.
You can use your favorite language, but we prefer C/C++, Rust, Go, or Python.

**提示：**
- 注意代码可读性，添加必要的注释（英文）
- 注意代码风格与规范，添加必要的单元测试和文档
- 注意异常处理，尝试优化性能

**Approach**

Join then Sum

Reduce random accesses, take advantage of caches and squential read/write.

Partition parallel join.

filter out useless data, reduce I/Os.