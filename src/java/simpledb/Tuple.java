package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema
 * specified by a TupleDesc object and contain Field objects with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private RecordId recordId;
    private Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     * As long as init the fields array.
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one
     *           field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) throws IllegalArgumentException {
        if (i >= this.tupleDesc.numFields() || i < 0) {
            throw new IllegalArgumentException();
        } else {
            this.fields[i] = f;
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i >= this.tupleDesc.numFields() || i < 0) {
            throw new IllegalArgumentException();
        } else {
            return this.fields[i];
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format
     * needs to be as follows:
     *
     * <p>column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * <p>where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            stringBuilder.append(fields[i]);
            if (i != fields.length - 1) {
                stringBuilder.append(' ');
            } else {
                stringBuilder.append('\n');
            }
        }
        return stringBuilder.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return new Iterator<Field>() {
            private int position = 0;

            @Override
            public boolean hasNext() {
                return position < fields.length;

            }

            @Override
            public Field next() {
                if (!hasNext()) {
                    return null;
                } else {
                    return fields[position++];
                }
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = new TupleDesc(new ArrayList<>());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tuple tuple = (Tuple) o;
        return Objects.equals(tupleDesc, tuple.tupleDesc) && Objects.equals(recordId, tuple.recordId) && Arrays.equals(fields, tuple.fields);
    }

}
