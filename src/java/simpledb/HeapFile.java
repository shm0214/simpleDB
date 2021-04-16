package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.tupleDesc = td;
        this.file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) throws IllegalArgumentException {
        int pageSize = BufferPool.getPageSize();
        byte[] pageData = new byte[pageSize];
        HeapPage page;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r")) {
            int position = pid.getPageNumber() * pageSize;
            randomAccessFile.seek(position);
            randomAccessFile.read(pageData, 0, pageSize);
            page = new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        return page;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        if (page.getId().getPageNumber() > numPages()) {
            throw new IllegalArgumentException();
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r");
        int pageSize = BufferPool.getPageSize();
        int position = page.getId().getPageNumber() * pageSize;
        randomAccessFile.seek(position);
        byte[] pageData = new byte[pageSize];
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pageList = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                pageList.add(page);
                return pageList;
            }
        }
        OutputStream out = new FileOutputStream(file, true);
        out.write(HeapPage.createEmptyPageData());
        out.flush();
        out.close();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        page.insertTuple(t);
        pageList.add(page);
        return pageList;

    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pageList = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pageList.add(page);
        return pageList;
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private Iterator<Tuple> iterator;
        private TransactionId transactionId;
        private int pagePosition = 0;

        public HeapFileIterator(TransactionId tid) {
            this.transactionId = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePosition = 0;
            iterator = getTupleIterator();
        }

        private Iterator<Tuple> getTupleIterator() throws TransactionAbortedException, DbException {
            HeapPageId heapPageId = new HeapPageId(getId(), pagePosition);
            if (pagePosition >= 0 && pagePosition < numPages()) {
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            } else {
                throw new DbException("Page Position error");
            }
        }

        @Override
        //这个hasNext是整个File中Tuple的下一个，因此需要检查当前页tuple的下一个和有没有剩余页
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return false;
            }
            if (iterator.hasNext()) {
                return true;
            } else {
                if (pagePosition < numPages() - 1) {
                    pagePosition++;
                    iterator = getTupleIterator();
                    return iterator.hasNext();
                } else {
                    return false;
                }

            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null) {
                throw new NoSuchElementException("Null Iterator Error");
            }
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

