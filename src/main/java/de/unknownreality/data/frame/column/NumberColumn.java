package de.unknownreality.data.frame.column;

import de.unknownreality.data.frame.DataFrameColumn;

import java.lang.reflect.Array;

/**
 * Created by Alex on 11.03.2016.
 */
public abstract class NumberColumn<T extends Number & Comparable<T>> extends BasicColumn<T> {
    public abstract  T median();

    public NumberColumn(String name){
       super(name);
    }

    public NumberColumn(){
        super(null);
    }

    public NumberColumn(String name, T[] values) {
        super(name,values);
    }
    @Override
    public T get(int index){
        return super.values[index];
    }

    public abstract NumberColumn<T> add(NumberColumn column);

    public abstract NumberColumn<T> subtract(NumberColumn column);

    public abstract NumberColumn<T> multiply(NumberColumn column);

    public abstract NumberColumn<T> divide(NumberColumn column);

    public abstract NumberColumn<T> add(Number value);

    public abstract NumberColumn<T> subtract(Number value);

    public abstract NumberColumn<T> multiply(Number value);

    public abstract NumberColumn<T> divide(Number value);

    public abstract Double mean();

    public abstract T min();

    public abstract T max();

    public abstract T sum();
}
