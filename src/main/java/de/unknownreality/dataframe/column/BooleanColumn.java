package de.unknownreality.dataframe.column;


import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.MapFunction;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class BooleanColumn extends DataFrameColumn<Boolean> {
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

    private Parser parser = ParserUtil.findParserOrNull(Boolean.class);

    @Override
    public Parser<Boolean> getParser() {
        return parser;
    }

    @Override
    public Class<Boolean> getType() {
        return Boolean.class;
    }


    @Override
    public BooleanColumn set(int index, Boolean value) {
        bitSet.set(index, value);
        return this;
    }

    @Override
    public DataFrameColumn<Boolean> map(MapFunction<Boolean> dataFunction) {
        for (int i = 0; i < size(); i++) {
            bitSet.set(i, dataFunction.map(bitSet.get(i)));
        }
        notifyDataFrameColumnChanged();
        return this;
    }

    @Override
    public void reverse() {
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
        this.bitSet = bitSet;
        setName(name);
        size = 0;
    }

    @Override
    public Boolean get(int index) {
        return bitSet.get(index);
    }

    @Override
    public DataFrameColumn<Boolean> copy() {
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
        if (!(o instanceof Boolean)) {
            return false;
        }
        return ((Boolean) o) ? !bitSet.isEmpty() : bitSet.isEmpty();
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

            ;
        };
    }

    @Override
    public Object[] toArray() {
        Boolean[] array = new Boolean[size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = bitSet.get(i);
        }
        return array;
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        if (a.length < size())
            return (T1[]) Arrays.copyOf(toArray(), size(), a.getClass());
        System.arraycopy(toArray(), 0, a, 0, size());
        if (a.length > size())
            a[size()] = null;
        return a;
    }


    @Override
    public boolean append(Boolean t) {
        validateAppend();
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
    public boolean appendAll(Collection<Boolean> c) {
        throw new UnsupportedOperationException("appendAll is not supported by dataframe column");
    }

    @Override
    public void appendNA() {
        append(false);
    }

    @Override
    public boolean isNA(int index) {
        return false;
    }

    @Override
    public void setNA(int index) {
        set(index, false);
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
