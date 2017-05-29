/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.common.header;

import de.unknownreality.dataframe.common.Header;

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

    /**
     * Returns <tt>true</tt> if this header contains the specified column header name
     *
     * @param name column name
     * @return <tt>true</tt> if this header contains the column name
     */
    @Override
    public boolean contains(String name) {
        return headerMap.containsKey(name);
    }

    /**
     * Returns the index of a specific entry.
     * throws an {@link IllegalArgumentException} if the column header name is not found
     *
     * @param name searched entry
     * @return index of entry
     */
    @Override
    public int getIndex(String name) {
        Integer index;
        if ((index = headerMap.get(name)) == null) {
            throw new IllegalArgumentException(String.format("column header name not found '%s'", name));
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
