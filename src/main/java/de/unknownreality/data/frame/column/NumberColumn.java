package de.unknownreality.data.frame.column;

/**
 * Created by Alex on 11.03.2016.
 */
public interface NumberColumn<T extends Number & Comparable<T>> extends DataColumn<T> {
    T median();

    T get(int index);

    NumberColumn<T> add(NumberColumn column);

    NumberColumn<T> subtract(NumberColumn column);

    NumberColumn<T> multiply(NumberColumn column);

    NumberColumn<T> divide(NumberColumn column);

    NumberColumn<T> add(Number value);

    NumberColumn<T> subtract(Number value);

    NumberColumn<T> multiply(Number value);

    NumberColumn<T> divide(Number value);

    Double mean();

    T min();

    T max();

    T sum();
}
