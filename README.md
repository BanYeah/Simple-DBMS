# Simple DBMS
## About
For the SimpleDB project description, please refer to ```SIMPLEDB.md```.

### Task 1. Improving Buffer Manager
Enhance the buffer manager to use an **LRU** (Least Recently Used) **replacement strategy** instead of a simple first-unpinned approach. Replace sequential block lookups with a hashmap for faster buffer allocation and add a ```printStatus``` method to display the buffer pool’s current state.

### Task 2. Wait-Die Scheme
Modify the lock manager to replace the existing timeout-based deadlock detection with the wait-die scheme. Implement transaction id comparisons to decide whether a transaction should wait or abort during lock acquisition.

### Task 3. Implement Additional Relational Algebra Operators
Extend SimpleDB with the **union** and **rename** relational algebra operators. The union operator outputs the union of records from two tables with the same schema, including duplicates. The rename operator allows field renaming to facilitate schema manipulation and compatibility.

> @SKKU (Sungkyunkwan University)