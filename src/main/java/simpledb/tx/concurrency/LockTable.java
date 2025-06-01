package simpledb.tx.concurrency;

import java.util.*;
import simpledb.file.BlockId;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 * @author Edward Sciore
 */
class LockTable {
   private final Map<BlockId, List<Integer>> locks = new HashMap<>();
   
   /**
    * Grant an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the lock is released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   public synchronized void sLock(BlockId blk, int txnum) { // slock: shared lock (Read-only)
      while(true) { // wait
         if (locks.get(blk) == null) { // unlock
            locks.put(blk, new ArrayList<>());
            locks.get(blk).add(txnum);
            break;
         } else if (locks.get(blk).get(0) < 0) { // xlock
            if (-locks.get(blk).get(0) < txnum)
               throw new LockAbortException(); // die
            else // wait
               try {
                  wait();
               } catch (InterruptedException e) {
                  throw new LockAbortException();
               }
         } else { // slock
            locks.get(blk).add(txnum);
            break;
         }
      }
   }
   
   /**
    * Grant an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the locks are released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   synchronized void xLock(BlockId blk, int txnum) { // xlock: Exclusive lock (Write-only)
      while(true) { // wait
         if (locks.get(blk) == null) { // unlock
            continue;
         } else if (locks.get(blk).get(0) < 0) { // xlock
            if (-locks.get(blk).get(0) < txnum)
               throw new LockAbortException(); // die
            else // wait
               try {
                  wait();
               } catch (InterruptedException e) {
                  throw new LockAbortException();
               }
         } else { // slock
            if (locks.get(blk).size() == 1 && locks.get(blk).get(0) == txnum) { // slock with same txnum
               locks.get(blk).set(0, -txnum); // upgrade it to xlock
               break;
            }

            Collections.sort(locks.get(blk));
            if (locks.get(blk).get(0) < txnum)
               throw new LockAbortException(); // die
            else // wait
               try {
                  wait();
               } catch (InterruptedException e) {
                  throw new LockAbortException();
               }
         }
      }
   }
   
   /**
    * Release a lock on the specified block.
    * If this lock is the last lock on that block,
    * then the waiting transactions are notified.
    * @param blk a reference to the disk block
    */
   synchronized void unlock(BlockId blk, int txnum) {
      if (locks.get(blk).get(0) < 0) // xlock
         if (locks.get(blk).get(0) == -txnum) {
            locks.remove(blk);
            notifyAll();
         }
      else { // slock
         locks.get(blk).remove((Integer) txnum);
         if (locks.get(blk).isEmpty())
            locks.remove(blk);
         notifyAll();
      }
   }
}
