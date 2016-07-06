package de.unknownreality.dataframe.frame.column;


import de.unknownreality.dataframe.frame.DataFrameColumn;
import de.unknownreality.dataframe.frame.MapFunction;
import de.unknownreality.dataframe.frame.Values;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class BasicColumn<T extends Comparable<T>> extends DataFrameColumn<T> {
    private static double GROW_FACTOR = 1.6d;
    public static final int INIT_SIZE = 128;

    private int size = 0;

    public void sort(Comparator<T> comparator) {
        Arrays.sort(values,0,size(), comparator);
        notifyDataFrameColumnChanged();
    }

    public void sort() {
        Arrays.sort(values,0,size());
        notifyDataFrameColumnChanged();
    }

    @Override
    public final DataFrameColumn<T> set(int index, T value) {
        if(value == Values.NA){
            appendNA();
            return this;
        }
        values[index] = value;
        notifyDataFrameValueChanged(index);
        return this;
    }

    @Override
    public DataFrameColumn<T> map(MapFunction<T> dataFunction) {
        for (int i = 0; i < size(); i++) {
            if(isNA(i)){
                continue;
            }
            values[i] = dataFunction.map(values[i]);
        }
        notifyDataFrameColumnChanged();
        return this;
    }

    protected T[] values;

    @Override
    public void reverse() {
        for (int i = 0; i < size() / 2; i++) {
            T temp = values[i];
            values[i] = values[size() - i - 1];
            values[size() - i - 1] = temp;
        }
        notifyDataFrameColumnChanged();
    }


    public BasicColumn(String name){
        this.size = 0;
        setName(name);
        values = (T[]) Array.newInstance(getType(), INIT_SIZE);
    }

    public BasicColumn(){
        this(null);
    }

    public BasicColumn(String name, T[] values) {
        this.values = values;
        setName(name);
        size = values.length;
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
        return new HashSet<T>(Arrays.asList(values)).contains(o);
    }

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
    public Object[] toArray() {
        return values;
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        if (a.length < size())
            return (T1[]) Arrays.copyOf(values, size(), a.getClass());
        System.arraycopy(values, 0, a, 0, size());
        if (a.length > size())
            a[size()] = null;
        return a;
    }

    @Override
    public final boolean append(T t) {
        validateAppend();
        if(size == values.length - 1){
            values = Arrays.copyOf(values, (int)((double)values.length * GROW_FACTOR));
        }
        values[size++] = t;
        return true;
    }

    @Override
    public void appendNA() {
        append(null);
    }

    @Override
    public boolean isNA(int index) {
        return values.length <= index || values[index] == null;
    }

    @Override
    public void setNA(int index) {
        values[index] = null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return new HashSet<T>(Arrays.asList(values)).containsAll(c);
    }


    @Override
    public boolean appendAll(Collection<T> c) {
        for(T o : c){
            append(o);
        }
        return true;
    }

    @Override
    public void clear() {
        values = (T[]) Array.newInstance(getType(), INIT_SIZE);
        size = 0;
    }

}
