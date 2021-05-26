package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int maxPageNumber = DEFAULT_PAGES;

    private final ConcurrentHashMap<PageId, Page> pageConcurrentHashMap;

    private final LockManager lockManager;

    private class Lock {
        TransactionId transactionId;
        int type; // 0 shared 1 exclusive

        public Lock(TransactionId tid, int type) {
            this.transactionId = tid;
            this.type = type;
        }
    }

    private class LockManager {
        private ConcurrentHashMap<PageId, List<Lock>> lockHashMap;

        public LockManager() {
            this.lockHashMap = new ConcurrentHashMap<PageId, List<Lock>>();
        }

        public synchronized boolean lock(TransactionId tid, PageId pid, int type) {
            List<Lock> locks = lockHashMap.get(pid);
            // 如果还没有锁
            if (locks == null) {
                locks = new ArrayList<>();
                Lock lock = new Lock(tid, type);
                locks.add(lock);
                lockHashMap.put(pid, locks);
                return true;
            }
            for (Lock lock : locks) {
                if (lock.transactionId.equals(tid)) {
                    // 已经有同级/更高级锁
                    if (lock.type == type || lock.type == 1) {
                        return true;
                    }
                    // 独占sharedLock，升级为exclusiveLock
                    if (locks.size() == 1) {
                        lock.type = 1;
                        return true;
                    }
                    // 本来是sharedLock想提升为exclusiveLock，但还有其他tx有sharedLock
                    return false;
                }
            }
            // 没锁并且已有其他tx的exclusiveLock
            if (locks.get(0).type == 1) {
                assert locks.size() == 1;
                return false;
            }
            // 没有exclusiveLock并且只想加一个sharedLock
            if (type == 0) {
                Lock lock = new Lock(tid, type);
                locks.add(lock);
                lockHashMap.put(pid, locks);
                return true;
            }
            // 想加exclusiveLock但有其他tx的sharedLock
            return false;
        }

        public synchronized boolean unlock(TransactionId tid, PageId pid) throws DbException {
            List<Lock> locks = lockHashMap.get(pid);
            if (locks == null || locks.size() == 0) {
                throw new DbException("empty lock map");
            }
            for (Lock lock : locks) {
                if (lock.transactionId.equals(tid)) {
                    locks.remove(lock);
                    if (locks.size() == 0) {
                        lockHashMap.remove(pid);
                    }
                    return true;
                }
            }
            return false;
        }

        public synchronized boolean holdLock(TransactionId tid, PageId pid) {
            List<Lock> locks = lockHashMap.get(pid);
            if (locks == null || locks.size() == 0) {
                return false;
            }
            for (Lock lock : locks) {
                if (lock.transactionId == tid) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxPageNumber = numPages;
        pageConcurrentHashMap = new ConcurrentHashMap<>(numPages);
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        int type = perm == Permissions.READ_ONLY ? 0 : 1;
        boolean isLocked = lockManager.lock(tid, pid, type);
        long start = System.currentTimeMillis();
        while (!isLocked) {
            long end = System.currentTimeMillis();
            if (end - start > 300) {
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            isLocked = lockManager.lock(tid, pid, type);
        }
        if (pageConcurrentHashMap.containsKey(pid)) {
            return pageConcurrentHashMap.get(pid);
        } else {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            if (pageConcurrentHashMap.size() < maxPageNumber) {
                pageConcurrentHashMap.put(pid, page);
            } else {
//                throw new DbException("no space for new page!");
                evictPage();
            }
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        try {
            lockManager.unlock(tid, pid);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            restorePages(tid);
        }
        for (PageId pageId : pageConcurrentHashMap.keySet()) {
            if (holdsLock(tid, pageId)) {
                releasePage(tid, pageId);
            }
        }
    }

    public synchronized void restorePages(TransactionId tid) {
        for (PageId pageId : pageConcurrentHashMap.keySet()) {
            Page page = pageConcurrentHashMap.get(pageId);
            if (page.isDirty() == tid) {
                DbFile f = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                pageConcurrentHashMap.put(pageId, f.readPage(page.getId()));
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            pageConcurrentHashMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            pageConcurrentHashMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : pageConcurrentHashMap.values()) {
            flushPage(page.getId());
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        pageConcurrentHashMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pageConcurrentHashMap.get(pid);
        if (page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (PageId pageId : pageConcurrentHashMap.keySet()) {
            Page page = pageConcurrentHashMap.get(pageId);
            if (page.isDirty() == tid) {
                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        /*
        PageId[] pageIds = pageConcurrentHashMap.keySet().toArray(new PageId[0]);
        Random random = new Random();
        PageId pageId = null;
        do {
            pageId = pageIds[random.nextInt(pageIds.length)];
        } while (pageConcurrentHashMap.get(pageId).isDirty() != null);
        if (pageConcurrentHashMap.get(pageId).isDirty() != null) {
            throw new DbException("All pages are dirty!");
        }
        */
        // 随机的效果太差了，换成从头遍历的
        PageId pageId = null;
        Page page = null;
        for (PageId pageid : pageConcurrentHashMap.keySet()) {
            page = pageConcurrentHashMap.get(pageid);
            pageId = pageid;
            if (page.isDirty() == null) {
                break;
            }
        }
        assert page != null;
        if (page.isDirty() != null) {
            throw new DbException("All pages are dirty");
        }
        // 由于换出的不是dirty的，也就没必要flush了
//        try {
//            flushPage(pageId);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        discardPage(pageId);
    }

}
