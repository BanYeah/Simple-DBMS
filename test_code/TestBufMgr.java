package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;
import java.util.*;

public class TestBufMgr {
   private static Map<BlockId,Buffer> buffs = new HashMap<>();
   private static BufferMgr bm;
   
   public static void main(String args[]) throws Exception {
      SimpleDB db = new SimpleDB("buffermgrtest", 400, 8);
      bm = db.bufferMgr();
      pinBuffer(0); pinBuffer(1); pinBuffer(2); pinBuffer(3);
      pinBuffer(4); pinBuffer(5); pinBuffer(6); pinBuffer(7);
      bm.printStatus();
      unpinBuffer(2); unpinBuffer(0); unpinBuffer(5); unpinBuffer(4);
      bm.printStatus();
      pinBuffer(8); pinBuffer(5); pinBuffer(7);
      bm.printStatus();
   }
   
   private static void pinBuffer(int i) {
      BlockId blk = new BlockId("test", i);
      Buffer buff = bm.pin(blk);
      buffs.put(blk, buff);
      System.out.println("Pin block " + i);
   }
   
   private static void unpinBuffer(int i) {
      BlockId blk = new BlockId("test", i);
      Buffer buff = buffs.remove(blk);
      bm.unpin(buff);
      System.out.println("Unpin block " + i);
   }
}

/* Output from correct implementation

Pin block 0
Pin block 1
Pin block 2
Pin block 3
Pin block 4
Pin block 5
Pin block 6
Pin block 7
Allocated Buffers:
Buffer 5: [file test, block 5] pinned
Buffer 4: [file test, block 4] pinned
Buffer 7: [file test, block 7] pinned
Buffer 6: [file test, block 6] pinned
Buffer 1: [file test, block 1] pinned
Buffer 0: [file test, block 0] pinned
Buffer 3: [file test, block 3] pinned
Buffer 2: [file test, block 2] pinned
Unpinned Buffers in LRU order:
Unpin block 2
Unpin block 0
Unpin block 5
Unpin block 4
Allocated Buffers:
Buffer 5: [file test, block 5] unpinned
Buffer 4: [file test, block 4] unpinned
Buffer 7: [file test, block 7] pinned
Buffer 6: [file test, block 6] pinned
Buffer 1: [file test, block 1] pinned
Buffer 0: [file test, block 0] unpinned
Buffer 3: [file test, block 3] pinned
Buffer 2: [file test, block 2] unpinned
Unpinned Buffers in LRU order: 2 0 5 4
Pin block 8
Pin block 5
Pin block 7
Allocated Buffers:
Buffer 2: [file test, block 8] pinned
Buffer 5: [file test, block 5] pinned
Buffer 4: [file test, block 4] unpinned
Buffer 7: [file test, block 7] pinned
Buffer 6: [file test, block 6] pinned
Buffer 1: [file test, block 1] pinned
Buffer 0: [file test, block 0] unpinned
Buffer 3: [file test, block 3] pinned
Buffer 2: [file test, block 8] pinned
Unpinned Buffers in LRU order: 0 4
 */
