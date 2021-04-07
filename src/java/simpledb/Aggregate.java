package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int aggregatorField;
    private int groupByField;
    private Aggregator.Op aggregatorOp;
    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggregatorField = afield;
        this.groupByField = gfield;
        this.aggregatorOp = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (this.groupByField == -1) {
            return null;
        } else {
            return this.child.getTupleDesc().getFieldName(groupByField);
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggregatorField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(aggregatorField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aggregatorOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        this.child.open();
        super.open();
        Aggregator aggregator;
        if (this.child.getTupleDesc().getFieldType(aggregatorField) == Type.INT_TYPE) {
            if (groupByField == -1) {
                aggregator = new IntegerAggregator(groupByField, null, aggregatorField, aggregatorOp);
            } else {
                aggregator = new IntegerAggregator(groupByField, child.getTupleDesc().getFieldType(groupByField), aggregatorField, aggregatorOp);
            }
        } else {
            if (groupByField == -1) {
                aggregator = new StringAggregator(groupByField, null, aggregatorField, aggregatorOp);
            } else {
                aggregator = new StringAggregator(groupByField, child.getTupleDesc().getFieldType(groupByField), aggregatorField, aggregatorOp);
            }
        }
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        if (groupByField == -1) {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{child.getTupleDesc().getFieldName(aggregatorField)});
        } else {
            return new TupleDesc(new Type[]{this.child.getTupleDesc().getFieldType(aggregatorField), Type.INT_TYPE}, new String[]{child.getTupleDesc().getFieldName(groupByField), child.getTupleDesc().getFieldName(aggregatorField)});
        }
    }

    @Override
    public void close() {
        iterator = null;
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
