package de.unknownreality.data.common;

/**
 * Created by Alex on 10.03.2016.
 */
public interface Header<T> extends Iterable<T>{
    public int size();
    public T get(int index);
    public boolean contains(T value);
    public int getIndex(T name);
}
