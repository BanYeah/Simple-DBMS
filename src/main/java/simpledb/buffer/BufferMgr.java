package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;

import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BufferMgr {
   private final LinkedList<Buffer> unpinnedBuffers; /* LRU list */
   private final LinkedList<Buffer> unallocatedBuffers;
   private final Map<BlockId, Buffer> allocatedBuffers;
   private int numAvailable;                    /* the number of available (unpinned) buffer slots */
   private static final long MAX_TIME = 10000;  /* 10 seconds */
   
   /**
    * Constructor:  Creates a buffer manager having the specified 
    * number of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      unpinnedBuffers = new LinkedList<>();
      unallocatedBuffers = new LinkedList<>();
      for (int i = 0; i < numbuffs; i++)
         unallocatedBuffers.addLast(new Buffer(fm, lm, i));

      allocatedBuffers = new HashMap<>();
      numAvailable = numbuffs;
   }

   /**
    * Displays its current status.
    * The status consists of the ID, block, and pinned status of each buffer in the allocated map,
    * plus the IDs of each buffer in the unpinned list.
    */
   public synchronized void printStatus() {
      System.out.println("Allocated Buffers:");
      for (Buffer buff : allocatedBuffers.values()) {
         System.out.print("Buffer " + buff.getId() + ": " + buff.block().toString());
         if (buff.isPinned())
            System.out.println(" pinned");
         else
            System.out.println(" unpinned");
      }

      System.out.print("Unpinned Buffers in LRU order: ");
      for (Buffer buff : unpinnedBuffers)
         System.out.print(buff.getId() + " ");
      System.out.println();
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public synchronized int available() {
      return numAvailable;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   public synchronized void flushAll(int txnum) {
      for (Buffer buff : allocatedBuffers.values())
         if (buff.modifyingTx() == txnum)
            buff.flush();
   }
   
   /**
    * Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) { // unpinned
         unpinnedBuffers.addLast(buff); // add it to the end of the LRU list
         numAvailable++;
         notifyAll(); // notify any waiting threads
      }
   }
   
   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed 
    * time period, then a {@link BufferAbortException} is thrown.
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) { // try for 10 seconds
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }
   
   /**
    * Returns true if starttime is older than 10 seconds
    * @param starttime timestamp 
    * @return true if waited for more than 10 seconds
    */
   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
   
   /**
    * Tries to pin a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseReplacementBuffer();
         if (buff == null)
            return null;

         buff.assignToBlock(blk);
         allocatedBuffers.remove(blk); // the mapping for the old block must be removed, and
         allocatedBuffers.put(blk, buff); // the mapping for the new block must be added.
      }
      if (!buff.isPinned()) { // unpinned
         unpinnedBuffers.remove(buff); // remove from LRU list
         numAvailable--;
      }
      buff.pin();
      return buff;
   }
   
   /**
    * Find and return a buffer assigned to the specified block.
    * @param blk a reference to a disk block
    * @return the found buffer
    */
   private Buffer findExistingBuffer(BlockId blk) {
      for (Buffer buff : allocatedBuffers.values()) {
         BlockId b = buff.block(); // return BlockID
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }
   
   /**
    * Find and return an available buffer (either unallocated or unpinned) for replacement.
    * @return the replacement buffer
    */
   private Buffer chooseReplacementBuffer() {
      if (!unallocatedBuffers.isEmpty())
         return unallocatedBuffers.removeFirst();
      else if (!unpinnedBuffers.isEmpty())
         return unpinnedBuffers.removeFirst(); // remove the buffer at the head of the list
      else
         return null;
   }
}
