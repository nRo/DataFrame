package de.unknownreality.dataframe.common;

/**
 * Created by Alex on 10.03.2016.
 */
public interface Header<T> extends Iterable<T> {
    /**
     * Returns the number of entries(columns) in this header
     *
     * @return number of entries
     */
    int size();

    /**
     * Gets the entry at a specific index
     *
     * @param index index of entry
     * @return entry at specific index
     */
    T get(int index);

    /**
     * Returns <tt>true</tt> if the header contains a specific entry
     *
     * @param value entry to be tested
     * @return <tt>true</tt> if header contains entry
     */
    boolean contains(T value);

    /**
     * Returns the index of a specific entry in this header
     *
     * @param name searched entry
     * @return index of the entry
     */
    int getIndex(T name);
}
