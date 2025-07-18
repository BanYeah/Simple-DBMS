package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * An individual buffer. A databuffer wraps a page 
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 * @author Edward Sciore
 */
public class Buffer {
   private final int id;
   private final FileMgr fm;     /* File manager */
   private final LogMgr lm;      /* Log manager */
   private final Page contents;  /* buffered page */
   private BlockId blk = null;   /* ID of the block */
   private int pins = 0;         /* the number of times the buffer has been pinned */
   private int txnum = -1;       /* dirty flag. The ID of the modifying transaction for this buffer */
   private int lsn = -1;         /* log sequence number */

   /**
    * Constructor 
    */
   public Buffer(FileMgr fm, LogMgr lm, int id) {
      this.id = id;
      this.fm = fm;
      this.lm = lm;
      contents = new Page(fm.blockSize());
   }

   /**
    * Returns the id of this buffer
    */
   public int getId() {
      return id;
   }
   
   /**
    * Returns the contents (i.e., page) of this buffer
    */
   public Page contents() {
      return contents;
   }

   /**
    * Returns a reference to the disk block
    * allocated to the buffer.
    * @return a reference to a disk block
    */
   public BlockId block() {
      return blk;
   }

   /**
    * Set the ID of the transaction that modified this buffer and 
    * set its log sequence number.
    * @param txnum the ID of the transaction      
    * @param lsn the log sequence number
    */
   public void setModified(int txnum, int lsn) {
      this.txnum = txnum;
      if (lsn >= 0)
         this.lsn = lsn;
   }

   /**
    * Return true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   public boolean isPinned() {
      return pins > 0;
   }
   
   /**
    * Return the ID of the modifying transaction 
    * @return -1 if this buffer is not dirty
    */
   public int modifyingTx() {
      return txnum;
   }

   /**
    * Reads the contents of the specified block into
    * the contents of the buffer.
    * If the buffer was dirty, then its previous contents
    * are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(BlockId b) {
      flush(); // if the buffer is dirty, flush to disk
      blk = b;
      fm.read(blk, contents);  // read the contents of the specified block
      pins = 0;
   }
   
   /**
    * If the buffer is dirty, write the log entry in a log file 
    * and write the buffer to its disk block 
    */
   void flush() {
      if (txnum >= 0) {            // if the buffer is dirty,
         lm.flush(lsn);            // write the log entry in a log file
         fm.write(blk, contents);  // write the buffer to its disk block
         txnum = -1;
      }
   }

   /**
    * Increase the buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decrease the buffer's pin count.
    */
   void unpin() {
      pins--;
   }
}
