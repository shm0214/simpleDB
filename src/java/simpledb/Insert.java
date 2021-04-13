package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator opIterator;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean first;
    private int count;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("tupleDesc mismatch");
        }
        this.transactionId = t;

        this.opIterator = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"count"});
        this.first = true;
        this.count = 0;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        opIterator.open();
        super.open();
        count = 0;
    }

    @Override
    public void close() {
        super.close();
        opIterator.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        opIterator.rewind();
        count = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!first) {
            return null;
        }
        first = false;
        while (opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, tuple);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        opIterator = children[0];
    }
}
