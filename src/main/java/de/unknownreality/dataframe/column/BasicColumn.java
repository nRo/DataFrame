package de.unknownreality.dataframe.column;


import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.MapFunction;
import de.unknownreality.dataframe.Values;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class BasicColumn<T extends Comparable<T>, C extends BasicColumn<T, C>> extends DataFrameColumn<T, C> {
    public static final double GROW_FACTOR = 1.6d;
    public static final int INIT_SIZE = 128;

    private int size = 0;

    @SuppressWarnings("unchecked")
    public BasicColumn(String name) {
        this.size = 0;
        setName(name);
        values = (T[]) Array.newInstance(getType(), INIT_SIZE);
    }

    public BasicColumn() {
        this(null);
    }

    public BasicColumn(String name, T[] values) {
        this.values = values;
        setName(name);
        size = values.length;
    }


    @Override
    protected void doSort(Comparator<T> comparator) {
        Arrays.sort(values, 0, size(), comparator);
    }


    @Override
    protected void doSort() {
        Arrays.sort(values, 0, size());
    }


    @Override
    protected final void doSet(int index, T value) {
        if (value == Values.NA) {
            doAppendNA();
            return;
        }
        values[index] = value;
    }


    @Override
    protected void doMap(MapFunction<T> mapFunction) {
        for (int i = 0; i < size(); i++) {
            if (isNA(i)) {
                continue;
            }
            values[i] = mapFunction.map(values[i]);
        }
    }

    protected T[] values;

    @Override
    protected void doReverse() {
        for (int i = 0; i < size() / 2; i++) {
            T temp = values[i];
            values[i] = values[size() - i - 1];
            values[size() - i - 1] = temp;
        }
    }


    @Override
    public T get(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return Arrays.asList(values).contains(o);
    }

    /**
     * Returns an iterator over the values in this column.
     * {@link Iterator#remove()} is not supported
     *
     * @return iterator over column values
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int index = 0;

            @Override
            public void remove() {

            }

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public T next() {
                return values[index++];
            }
        };
    }


    @Override
    public Comparable[] toArray() {
        return Arrays.copyOf(values, size());
    }


    @SuppressWarnings("unchecked")
    @Override
    public T[] toArray(T[] a) {
        if (a.length < size())
            return (T[]) Arrays.copyOf(values, size(), a.getClass());
        System.arraycopy(values, 0, a, 0, size());
        if (a.length > size())
            a[size()] = null;
        return a;
    }


    @Override
    protected boolean doAppend(T t) {
        if (size == values.length - 1) {
            values = Arrays.copyOf(values, (int) ((double) values.length * GROW_FACTOR));
        }
        values[size++] = t;
        return true;
    }

    @Override
    protected boolean doAppendNA() {
        return doAppend(null);

    }

    @Override
    public boolean isNA(int index) {
        return values.length <= index || values[index] == null;
    }


    @Override
    protected void doSetNA(int index) {
        values[index] = null;
    }


    @Override
    public boolean containsAll(Collection<?> c) {
        return new HashSet<>(Arrays.asList(values)).containsAll(c);
    }


    @Override
    protected boolean doAppendAll(Collection<? extends T> c) {
        for (T o : c) {
            doAppend(o);
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() {
        values = (T[]) Array.newInstance(getType(), INIT_SIZE);
        size = 0;
    }

}
