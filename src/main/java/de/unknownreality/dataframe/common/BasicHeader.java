package de.unknownreality.dataframe.common;

import java.util.*;

/**
 * Created by Alex on 15.03.2016.
 */
public class BasicHeader implements Header<String> {
    private final Map<String, Integer> headerMap = new HashMap<>();
    private final List<String> headers = new ArrayList<>();

    @Override
    public int size() {
        return headers.size();
    }

    /**
     * Adds an entry to this header
     *
     * @param name entry to be added
     */
    public void add(String name) {
        headerMap.put(name, headers.size());
        headers.add(name);
    }

    /**
     * Gets the entry at a specific index.
     * Throws an {@link IllegalArgumentException} if the index is out of bounds.
     *
     * @param index index of entry
     * @return entry at specific index
     */
    @Override
    public String get(int index) {
        if (index >= headers.size()) {
            throw new IllegalArgumentException(String.format("header index out of bounds %d > %d", index, (headers.size() - 1)));
        }
        return headers.get(index);
    }

    @Override
    public boolean contains(String name) {
        return headerMap.containsKey(name);
    }

    /**
     * Returns the index of a specific entry.
     * Returns <tt>-1</tt> if entry is not found in this header
     *
     * @param name searched entry
     * @return index if entry or <tt>-1</tt> if entry is not found
     */
    @Override
    public int getIndex(String name) {
        Integer index = headerMap.get(name);
        if (index == null) {
            return -1;
        }
        return index;
    }

    /**
     * Removes all entries from this header
     */
    public void clear() {
        headerMap.clear();
        headers.clear();
    }

    /**
     * Returns an iterator over the entries in this header
     * {@link Iterator#remove()} is not supported.
     *
     * @return iterator over entries
     */
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            final Iterator<String> it = headers.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }

            @Override
            public String next() {
                return it.next();
            }
        };

    }
}
