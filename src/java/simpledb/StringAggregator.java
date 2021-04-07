package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByField;
    private Type groupByFieldType;
    private int aggregatorField;
    private Op operator;

    private HashMap<Field, Integer> countMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregatorField = afield;
        this.operator = what;
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tup.getField(aggregatorField) == null) {
            return;
        }
        Field field = groupByField != NO_GROUPING ? tup.getField(this.groupByField) : null;
        if (this.countMap.containsKey(field)) {
            this.countMap.put(field, this.countMap.get(field) + 1);
        } else {
            this.countMap.put(field, 1);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new StringAggregateIterator();
    }

    private class StringAggregateIterator implements OpIterator {
        private Iterator<HashMap.Entry<Field, Integer>> iterator;
        private TupleDesc tupleDesc;

        public StringAggregateIterator() {
            if (groupByField == NO_GROUPING) {
                this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"count"});
            } else {
                this.tupleDesc = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE}, new String[]{"groupVal", "count"});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator = countMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Map.Entry<Field, Integer> entry = iterator.next();
            Field field = entry.getKey();
            Tuple tuple = new Tuple(this.tupleDesc);
            if (field == null) {
                tuple.setField(0, new IntField(entry.getValue()));
            } else {
                tuple.setField(0, field);
                tuple.setField(1, new IntField(entry.getValue()));
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = countMap.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}
