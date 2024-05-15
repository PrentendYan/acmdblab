# lab5

# 521120910245 严冬

## structure

1. `BTreeFile`: This class implements a database file that stores a B+ tree index. It provides methods for inserting, deleting, and searching tuples in the B+ tree. The key methods are:
   - `insertTuple`: Inserts a tuple into the B+ tree, potentially causing pages to split if they become full.
   - `deleteTuple`: Deletes a tuple from the B+ tree, merging or redistributing pages if they become less than half full.
   - `findLeafPage`: Finds and locks the leaf page corresponding to a given key field.
   - `splitLeafPage` and `splitInternalPage`: Methods for splitting leaf and internal pages when they become full.
   - `mergeLeafPages` and `mergeInternalPages`: Methods for merging leaf and internal pages when they become less than half full.
2. `BufferPool`: This class manages the reading and writing of pages into memory from disk. It is responsible for locking pages, evicting pages when the buffer is full, and ensuring the ACID properties of transactions. The key methods are:
   - `getPage`: Retrieves a page from the buffer pool, acquiring the necessary locks and potentially evicting other pages if the buffer is full.
   - `releasePage` and `transactionComplete`: Methods for releasing locks and committing or aborting transactions.
   - `insertTuple` and `deleteTuple`: Methods for inserting and deleting tuples, handling dirty pages and updating the buffer pool accordingly.
   - `flushAllPages` and `discardPage`: Methods for flushing pages to disk and removing pages from the buffer pool.
3. `HeapFile`: This class implements a database file that stores tuples in a heap file (an unordered collection of pages). It provides methods for reading, writing, inserting, and deleting tuples in the heap file.

1. **B+ Tree Operations**: Implementing various operations on a B+ tree index, such as insertion, deletion, splitting, and merging of pages.
2. **Buffer Pool Management**: Managing the buffer pool, which caches pages in memory, handling page eviction, and ensuring the ACID properties of transactions through locking and logging mechanisms.
3. **Heap File Operations**: Performing basic operations on a heap file, such as reading, writing, inserting, and deleting tuples.

## test

![image-20240515210340246](C:\Users\Lenovo\AppData\Roaming\Typora\typora-user-images\image-20240515210340246.png)

![image-20240515210450430](C:\Users\Lenovo\AppData\Roaming\Typora\typora-user-images\image-20240515210450430.png)