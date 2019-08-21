/*
 *
 *  * Copyright (c) 2019 Alexander Grün
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

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Header;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class BasicTypeHeader<T> implements Header<T> {
    private final Map<T, Integer> headerMap = new HashMap<>();
    private final List<T> headers = new ArrayList<>();
    private final Map<T, Class<? extends Comparable>> typesMap = new HashMap<>();
    private final Map<T, Class<? extends DataFrameColumn>> colTypeMap = new HashMap<>();

    @Override
    public int size() {
        return headers.size();
    }

    /**
     * Adds a new data frame column to this header
     *
     * @param headerName name of the added column
     * @param column new data frame column
     * @return <tt>self</tt> for method chaining
     */
    public BasicTypeHeader add(T headerName, DataFrameColumn<?, ?> column) {
        return add(headerName, column.getClass(), column.getType());
    }


    /**
     * Adds a new header entry based on column name, column class and column value type.
     *
     * @param name     column name
     * @param colClass column class
     * @param type     column value type
     * @return <tt>self</tt> for method chaining
     */
    public BasicTypeHeader add(T name, Class<? extends DataFrameColumn> colClass, Class<? extends Comparable> type) {
        int index = headers.size();
        headers.add(name);
        headerMap.put(name, index);
        typesMap.put(name, type);
        colTypeMap.put(name, colClass);
        return this;
    }

    /**
     * Adds a new header entry based on column name, column class and column value type.
     *
     * @param name     column name
     * @param colClass column class
     * @param type     column value type
     * @return <tt>self</tt> for method chaining
     */
    public BasicTypeHeader set(T name, Class<? extends DataFrameColumn> colClass, Class<? extends Comparable> type) {
        Integer index = headerMap.get(name);
        if (index == null) {
            add(name, colClass, type);
        }
        typesMap.put(name, type);
        colTypeMap.put(name, colClass);
        return this;
    }

    /**
     * Replaces an existing header with a new one.
     *
     * @param existing  existing column name
     * @param replacement  replacement column name
     * @param colClass replacement column class
     * @param type     replacement column value type
     * @return <tt>self</tt> for method chaining
     */
    public BasicTypeHeader replace(T existing, T replacement, Class<? extends DataFrameColumn> colClass, Class<? extends Comparable> type) {
        Integer index = headerMap.get(existing);
        if (index == null) {
            throw new DataFrameRuntimeException(String.format("header not found: %s",existing));
        }
        headers.remove(existing);
        headers.add(index,replacement);
        headerMap.put(replacement,index);
        typesMap.remove(existing);
        colTypeMap.remove(existing);
        typesMap.put(replacement, type);
        colTypeMap.put(replacement, colClass);
        return this;
    }


    /**
     * Removes a column from this header
     *
     * @param name column name
     */
    public void remove(T name) {
        boolean fix = false;
        for (T s : headers) {
            if (!fix && s.equals(name)) {
                fix = true;
                continue;
            }
            if (fix) {
                headerMap.put(s, headerMap.get(s) - 1);
            }
        }
        headers.remove(name);
        headerMap.remove(name);
        typesMap.remove(name);
        colTypeMap.remove(name);
    }

    /**
     * Renames a column
     *
     * @param oldName old name
     * @param newName new name
     */
    public void rename(T oldName, T newName) {
        for (int i = 0; i < headers.size(); i++) {
            if (headers.get(i).equals(oldName)) {
                headers.set(i, newName);
                Class<? extends Comparable> type = typesMap.get(oldName);
                typesMap.remove(oldName);
                typesMap.put(newName, type);

                Class<? extends DataFrameColumn> colType = colTypeMap.get(oldName);
                colTypeMap.remove(oldName);
                colTypeMap.put(newName, colType);

                Integer index = headerMap.get(oldName);
                headerMap.remove(oldName);
                headerMap.put(newName, index);
                return;
            }
        }
    }

    /**
     * Returns <tt>true</tt> if the other header is compatible with this header.
     * Compatible means that both headers contain the same columns with the same column classes.
     *
     * @param other object to test for compatibility
     * @return <tt>true</tt> if the object is equal or compatible with this header
     */
    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (other.getClass() != this.getClass()) {
            return false;
        }
        BasicTypeHeader otherHeader = (BasicTypeHeader) other;
        if (size() != otherHeader.size()) {
            return false;
        }
        for (T s : headers) {
            if (!otherHeader.contains(s)) {
                return false;
            }
            if (getType(s) != otherHeader.getType(s)) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns the column class for an input column name
     *
     * @param name input column name
     * @return column class
     */
    public Class<? extends DataFrameColumn> getColumnType(T name) {
        return colTypeMap.get(name);
    }

    /**
     * Returns the column class for a column index
     *
     * @param index column index
     * @return column class
     */
    public Class<? extends DataFrameColumn> getColumnType(int index) {
        return colTypeMap.get(get(index));
    }

    /**
     * Returns the column value type for an input column name
     *
     * @param name input column name
     * @return column value type
     */
    public Class<? extends Comparable> getType(T name) {
        return typesMap.get(name);
    }

    /**
     * Returns the column value type for a column index
     *
     * @param index column index
     * @return column value type
     */
    public Class<? extends Comparable> getType(int index) {
        return typesMap.get(get(index));
    }

    /**
     * Returns the column header name at a specific index.
     * Throws an {@link DataFrameRuntimeException} if the index is out of bounds.
     *
     * @param index index of column
     * @return column name at index
     */
    public T get(int index) {
        if (index >= headers.size()) {
            throw new DataFrameRuntimeException(String.format("header index out of bounds %d > %d", index, (headers.size() - 1)));
        }
        return headers.get(index);
    }

    /**
     * Returns <tt>true</tt> if the header contains a column with the input name
     *
     * @param name input name
     * @return <tt>true</tt> if header contains input name
     */
    @Override
    public boolean contains(T name) {
        return headerMap.containsKey(name);
    }


    /**
     * Clears this header
     */
    public void clear() {
        headerMap.clear();
        headers.clear();
        typesMap.clear();
    }

    /**
     * Returns the column index of a specific column name.
     * throws an {@link DataFrameRuntimeException} if the column header name is not found
     *
     * @param name searched column name
     * @return column index
     */
    public int getIndex(T name) {
        Integer index;
        if ((index = headerMap.get(name)) == null) {
            throw new DataFrameRuntimeException(String.format("column header name not found '%s'", name));
        }
        return index;
    }


    /**
     * Creates a copy of this header.
     *
     * @return copy of header
     */
    public abstract BasicTypeHeader copy();

    /**
     * Returns a string representation of this header
     *
     * @return string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("#");
        for (int i = 0; i < headers.size(); i++) {
            sb.append(headers.get(i));
            if (i < headers.size() - 1) {
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    /**
     * Returns an iterator over the column names in this header.
     * {@link Iterator#remove()} is not supported
     *
     * @return column name iterator
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int i = 0;

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }

            @Override
            public boolean hasNext() {
                return i != headers.size();
            }

            @Override
            public T next() {
                if (i >= headers.size()) {
                    throw new NoSuchElementException(String.format("element not found: index out of bounds %s >= %s]", i, headers.size()));
                }
                return headers.get(i++);
            }
        };
    }

}
