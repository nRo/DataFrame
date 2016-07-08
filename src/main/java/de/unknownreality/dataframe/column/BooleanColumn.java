package de.unknownreality.dataframe.column;


import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.MapFunction;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class BooleanColumn extends DataFrameColumn<Boolean, BooleanColumn> {
    private int size = 0;


    private BitSet bitSet = new BitSet();

    public BooleanColumn and(BooleanColumn other) {
        bitSet.and(other.bitSet);
        return this;
    }

    public BooleanColumn andNot(BooleanColumn other) {
        bitSet.andNot(other.bitSet);
        return this;
    }

    public BooleanColumn or(BooleanColumn other) {
        bitSet.or(other.bitSet);
        return this;
    }

    public BooleanColumn xor(BooleanColumn other) {
        bitSet.xor(other.bitSet);
        return this;
    }

    public BooleanColumn flip() {
        bitSet.flip(0, size());
        return this;
    }

    @Override
    protected BooleanColumn getThis() {
        return null;
    }

    private final Parser<Boolean> parser = ParserUtil.findParserOrNull(Boolean.class);

    @Override
    protected void doSort(Comparator<Boolean> comparator) {

    }

    @Override
    protected void doSort() {

    }

    @Override
    public Parser<Boolean> getParser() {
        return parser;
    }

    @Override
    public Class<Boolean> getType() {
        return Boolean.class;
    }


    @Override
    protected void doSet(int index, Boolean value) {
        bitSet.set(index, value);
    }

    @Override
    public void doMap(MapFunction<Boolean> mapFunction) {
        for (int i = 0; i < size(); i++) {
            bitSet.set(i, mapFunction.map(bitSet.get(i)));
        }
        notifyDataFrameColumnChanged();
    }

    @Override
    public void doReverse() {
        for (int i = 0; i < size() / 2; i++) {
            Boolean temp = bitSet.get(i);
            bitSet.set(i, bitSet.get(size - i - 1));
            bitSet.set(size() - i - 1, temp);
        }
        notifyDataFrameColumnChanged();
    }

    public BooleanColumn(String name) {
        this.size = 0;
        setName(name);
    }

    public BooleanColumn() {
        this(null);
    }

    public BooleanColumn(String name, Boolean[] values) {
        for (int i = 0; i < values.length; i++) {
            bitSet.set(i, values[i]);
        }
        setName(name);
        size = values.length;
    }

    public BooleanColumn(String name, BitSet values) {
        this.bitSet = values;
        setName(name);
        size = 0;
    }

    @Override
    public Boolean get(int index) {
        return bitSet.get(index);
    }

    @Override
    public BooleanColumn copy() {
        return null;
    }

    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return o instanceof Boolean && ((Boolean) o) != bitSet.isEmpty();
    }


    @Override
    public Iterator<Boolean> iterator() {
        return new Iterator<Boolean>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public Boolean next() {
                return bitSet.get(index++);
            }

            @Override
            public void remove() {
            }

        };
    }

    @Override
    public Boolean[] toArray() {
        Boolean[] array = new Boolean[size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = bitSet.get(i);
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Boolean[] toArray(Boolean[] a) {
        if (a.length < size())
            return (Boolean[]) Arrays.copyOf(toArray(), size(), a.getClass());
        System.arraycopy(toArray(), 0, a, 0, size());
        if (a.length > size())
            a[size()] = null;
        return a;
    }


    @Override
    protected boolean doAppend(Boolean t) {
        bitSet.set(size++, t);
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        Set<Object> set = new HashSet<>(c);
        for (Object o : set) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean doAppendAll(Collection<? extends Boolean> c) {
        for(Boolean b : c){
            bitSet.set(size++,b);
        }
        return true;
    }

    @Override
    protected boolean doAppendNA() {
        return doAppend(false);
    }

    @Override
    public boolean isNA(int index) {
        return false;
    }

    @Override
    protected void doSetNA(int index) {
        doSet(index, false);
    }

    @Override
    public void clear() {
        bitSet.clear();
        size = 0;
    }

    public Boolean[] getValues() {
        return toArray(new Boolean[size()]);
    }

}
