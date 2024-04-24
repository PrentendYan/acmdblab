package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.pageNumber();

        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file, "r");
            if ((pgNo + 1) * BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek(pgNo * BufferPool.getPageSize());
            // big end
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.pageNumber());
            return new HeapPage(id, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = (int) Math.floor((double)file.length() / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    // Helper class
    private final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId transactionId;
        private Iterator<Tuple> currentTupleIterator;
        private int currentPageIndex;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.transactionId = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPageIndex = 0;
            currentTupleIterator = getTupleIteratorForPage(currentPageIndex);
        }

        private Iterator<Tuple> getTupleIteratorForPage(int pageIndex) throws TransactionAbortedException, DbException {
            if (pageIndex >= 0 && pageIndex < heapFile.numPages()) {
                HeapPageId pageId = new HeapPageId(heapFile.getId(), pageIndex);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                return Collections.emptyIterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            while ((currentTupleIterator == null || !currentTupleIterator.hasNext()) && currentPageIndex < heapFile.numPages()) {
                currentPageIndex++;
                if (currentPageIndex < heapFile.numPages()) {
                    currentTupleIterator = getTupleIteratorForPage(currentPageIndex);
                }
            }
            return currentTupleIterator != null && currentTupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (currentTupleIterator == null || !currentTupleIterator.hasNext()) {
                throw new NoSuchElementException("No more tuples available.");
            }
            return currentTupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open(); // Reuse the open method to reset to the first page.
        }

        @Override
        public void close() {
            currentTupleIterator = null;
        }
    }
}
