package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByField;
    private Type groupByFieldType;
    private int aggregatorField;
    private Op operator;
    private HashMap<Field, Integer> countMap;
    private HashMap<Field, Integer> sumMap;
    private HashMap<Field, Integer> minMap;
    private HashMap<Field, Integer> maxMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregatorField = afield;
        this.operator = what;
        switch (operator) {
            case MIN:
                this.minMap = new HashMap<>();
                break;
            case MAX:
                this.maxMap = new HashMap<>();
                break;
            case COUNT:
                this.countMap = new HashMap<>();
                break;
            case SUM:
                this.sumMap = new HashMap<>();
                break;
            case AVG:
                this.sumMap = new HashMap<>();
                this.countMap = new HashMap<>();
                break;
            default:
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tup.getField(aggregatorField) == null) {
            return;
        }
        Field field = groupByField != NO_GROUPING ? tup.getField(this.groupByField) : null;
        Integer newValue = ((IntField) tup.getField(aggregatorField)).getValue();
        switch (operator) {
            case MIN:
                if (this.minMap.containsKey(field)) {
                    this.minMap.put(field, Math.min(this.minMap.get(field), newValue));
                } else {
                    this.minMap.put(field, newValue);
                }
                break;
            case MAX:
                if (this.maxMap.containsKey(field)) {
                    this.maxMap.put(field, Math.max(this.maxMap.get(field), newValue));
                } else {
                    this.maxMap.put(field, newValue);
                }
                break;
            case COUNT:
                if (this.countMap.containsKey(field)) {
                    this.countMap.put(field, this.countMap.get(field) + 1);
                } else {
                    this.countMap.put(field, 1);
                }
                break;

            case SUM:
                if (this.sumMap.containsKey(field)) {
                    this.sumMap.put(field, this.sumMap.get(field) + newValue);
                } else {
                    this.sumMap.put(field, newValue);
                }
                break;
            case AVG:
                if (this.sumMap.containsKey(field)) {
                    this.sumMap.put(field, this.sumMap.get(field) + newValue);
                    this.countMap.put(field, this.countMap.get(field) + 1);
                } else {
                    this.sumMap.put(field, newValue);
                    this.countMap.put(field, 1);
                }
                break;
            default:
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new IntegerAggregateIterator();
    }

    private class IntegerAggregateIterator implements OpIterator {
        private Iterator<HashMap.Entry<Field, Integer>> iterator, iterator1;
        private TupleDesc tupleDesc;

        public IntegerAggregateIterator() {
            if (groupByField == NO_GROUPING) {
                this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            } else {
                this.tupleDesc = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            switch (operator) {
                case MIN:
                    this.iterator = minMap.entrySet().iterator();
                    break;
                case MAX:
                    this.iterator = maxMap.entrySet().iterator();
                    break;
                case COUNT:
                    this.iterator = countMap.entrySet().iterator();
                    break;
                case SUM:
                    this.iterator = sumMap.entrySet().iterator();
                    break;
                case AVG:
                    this.iterator = sumMap.entrySet().iterator();
                    this.iterator1 = countMap.entrySet().iterator();
                    break;
                default:
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (operator != Op.AVG) {
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
            } else {
                Map.Entry<Field, Integer> entry = iterator.next();
                Map.Entry<Field, Integer> entry1 = iterator1.next();
                Field field = entry.getKey();
                Tuple tuple = new Tuple(this.tupleDesc);
                if (field == null) {
                    tuple.setField(0, new IntField(entry.getValue() / entry1.getValue()));
                } else {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(entry.getValue() / entry1.getValue()));
                }
                return tuple;
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;
            iterator1 = null;
        }
    }
}
