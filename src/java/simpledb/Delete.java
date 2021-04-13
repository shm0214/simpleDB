package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator opIterator;
    private TupleDesc tupleDesc;
    private boolean first;
    private int count;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.opIterator = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"count"});
        this.count = 0;
        this.first = true;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // 这里明明没有这个要求，但是不写就又通不过测试
        if (!first) {
            return null;
        }
        first = false;
        while (this.opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, tuple);
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
