package simpledb.tx.concurrency;

import java.util.*;
import simpledb.file.BlockId;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then the transaction either waits or is aborted
 * based on the wait-die scheme.
 * Under the wait-die scheme, older transactions wait, while younger
 * transactions are aborted to prevent deadlocks.
 * There is only one wait list for all blocks.
 * When the last lock on a block is released, then all transactions
 * waiting for that block are rescheduled.
 * If a transaction discovers that the lock it is waiting for
 * is still held by an older transaction, it is aborted and rolled back.
 * @author Edward Sciore
 */
class LockTable {
   private final Map<BlockId, List<Integer>> locks = new HashMap<>();
   
   /**
    * Grant an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling transaction will either wait or be aborted
    * based on the wait-die scheme.
    * Under the wait-die scheme, older transactions wait,
    * while younger transactions are aborted to prevent deadlocks.
    * @param blk a reference to the disk block
    * @param txnum the transaction id requesting the lock
    */
   public synchronized void sLock(BlockId blk, int txnum) { // slock: shared lock (Read-only)
      while(true) {
         List<Integer> holders = locks.get(blk);
         if (holders == null) { // unlock
            holders = new ArrayList<>();
            holders.add(txnum);
            locks.put(blk, holders);
            break;
         } else if (hasXlock(holders)) { // xlock
            int holderTx = -holders.get(0);
            if (holderTx < txnum)
               throw new LockAbortException(); // die
            else
               try {
                  wait(); // wait
               } catch (InterruptedException e) {
                  throw new LockAbortException();
               }
         } else { // slock
            holders.add(txnum); // compatible
            break;
         }
      }
   }
   
   /**
    * Grant an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling transaction will either wait or be aborted
    * based on the wait-die scheme.
    * Under the wait-die scheme, older transactions wait,
    * while younger transactions are aborted to prevent deadlocks.
    * @param blk a reference to the disk block
    * @param txnum the transaction id requesting the lock
    */
   synchronized void xLock(BlockId blk, int txnum) { // xlock: Exclusive lock (Write-only)
      while(true) {
         List<Integer> holders = locks.get(blk);
         if (holders == null) { // unlock
            continue;
         } else if (hasXlock(holders)) { // xlock
            int holderTx = -holders.get(0);
            if (holderTx < txnum)
               throw new LockAbortException(); // die
            else
               try {
                  wait(); // wait
               } catch (InterruptedException e) {
                  throw new LockAbortException();
               }
         } else { // slock
            // upgrade slock to xlock if only held by the same txnum
            if (holders.size() == 1 && holders.get(0) == txnum) {
               holders.set(0, -txnum);
               break;
            }

            for (int holderTx : holders) {
               if (holderTx < txnum)
                  throw new LockAbortException(); // die
            }

            try {
               wait(); // wait
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
    * @param txnum the transaction id requesting the lock
    */
   synchronized void unlock(BlockId blk, int txnum) {
      List<Integer> holders = locks.get(blk);
      if (hasXlock(holders)) // xlock
         if (holders.get(0) == -txnum) {
            locks.remove(blk);
            notifyAll();
         }
      else { // slock
         holders.remove((Integer) txnum);
         if (holders.isEmpty())
            locks.remove(blk);
         notifyAll();
      }
   }

   private boolean hasXlock(List<Integer> holders) {
      return holders.get(0) < 0;
   }
}
