package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> TDList;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this
     * TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return new Iterator<TDItem>() {
            private int position = 0;

            @Override
            public boolean hasNext() {
                return TDList.size() >= position;
            }

            @Override
            public TDItem next() {
                if (!hasNext()) {
                    return null;
                } else {
                    return TDList.get(position++);
                }
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with
     * associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this TupleDesc. It must
     *                contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int length = typeAr.length;
        TDList = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            TDList.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }


    /**
     * Create a new TupleDesc with an TDItem arrayList which is already existed.
     *
     * @param TDList an arrayList of TDItems.
     */
    public TupleDesc(ArrayList<TDItem> TDList) {
        this.TDList = TDList;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
     * types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
     *               contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int length = typeAr.length;
        TDList = new ArrayList<>(length);
        for (Type type : typeAr) {
            TDList.add(new TDItem(type, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= TDList.size()) {
            throw new NoSuchElementException();
        } else {
            return TDList.get(i).fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= TDList.size()) {
            throw new NoSuchElementException();
        } else {
            return TDList.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (TDItem item : TDList) {
            if (item.fieldName != null && item.fieldName.equals(name)) {
                return TDList.indexOf(item);
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from a
     * given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (TDItem item : TDList) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first
     * td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        ArrayList<TDItem> newList = new ArrayList<>(td1.numFields() + td2.numFields());
        newList.addAll(td1.TDList);
        newList.addAll(td2.TDList);
        return new TupleDesc(newList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered
     * equal if they have the same number of items and if the i-th type in this TupleDesc is equal to
     * the i-th type in o for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc) {
            TupleDesc tupleDesc = (TupleDesc) o;
            if (this.numFields() != tupleDesc.numFields()) {
                return false;
            } else {
                for (int i = 0; i < this.numFields(); i++) {
                    if (!this.TDList.get(i).fieldType.equals(tupleDesc.TDList.get(i).fieldType)) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does
     * not matter.
     *
     * @return String describing this descriptor.
     */

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        for (TDItem item : TDList) {
            string.append(item.toString());
            string.append(", ");
        }
        return string.toString();
    }
}
