package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.frame.MapFunction;

import java.util.Collection;

/**
 * Created by Alex on 11.03.2016.
 */
public interface DataColumn<T extends Comparable<T>> extends Iterable<T> {

    String getName();

    void setName(String name);

    Parser<T> getParser();

    Class<T> getType();

    DataColumn<T> set(int index, T value);

    DataColumn<T> map(MapFunction<T> dataFunction);

    void reverse();

    T get(int index);

    DataColumn<T> copy();

    public void clear();

    public <T1> T1[] toArray(T1[] a);

    public Object[] toArray();

    public boolean containsAll(Collection<?> c);

    public boolean contains(Object o);

    boolean append(T value);
    public int size();

    boolean isEmpty();
    public boolean appendAll(Collection<T> c);

    public void appendNA();

    public boolean isNA(int index);
    public void setNA(int index);
}
