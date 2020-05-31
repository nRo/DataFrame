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

package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.header.TypeHeader;
import de.unknownreality.dataframe.type.ValueType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Alex on 11.03.2016.
 */
public class GroupHeader implements TypeHeader<String> {
    private final Map<String, Integer> headerMap = new HashMap<>();
    private final String[] headers;
    private final ValueType<?>[] types;

    public GroupHeader(String[] columns, ValueType<?>[] valueTypes) {
        headers = new String[columns.length];
        types = new ValueType[columns.length];
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            headers[i] = column;
            types[i] = valueTypes[i];
            headerMap.put(column, i);
        }
    }

    @Override
    public ValueType<?> getValueType(int index) {
        return types[index];
    }

    @Override
    public ValueType<?> getValueType(String name) {
        return getValueType(headerMap.get(name));
    }

    /**
     * Returns the number of group values in this header
     *
     * @return number of group values
     */
    @Override
    public int size() {
        return headers.length;
    }

    /**
     * Gets the group value name at a specific index.
     * Throws an {@link DataFrameRuntimeException} if the index is out of bounds.
     *
     * @param index index of group value
     * @return entry at specific index
     */
    @Override
    public String get(int index) {
        if (index >= headers.length) {
            throw new DataFrameRuntimeException(
                    String.format("header index out of bounds %d > %d", index, (headers.length - 1)));
        }
        return headers[index];
    }

    /**
     * Returns <tt>true</tt> if this header contains the specified group value column name
     *
     * @param groupValueName group value column name
     * @return <tt>true</tt> if this header contains the column name
     */
    @Override
    public boolean contains(String groupValueName) {
        return headerMap.containsKey(groupValueName);
    }

    /**
     * Returns the index of a specific entry.
     * throws an {@link DataFrameRuntimeException} if the group value column name is not found
     *
     * @param name searched entry
     * @return index if entry
     */
    @Override
    public int getIndex(String name) {
        Integer index;
        if ((index = headerMap.get(name)) == null) {
            throw new DataFrameRuntimeException(String.format("column header name not found '%s'", name));
        }
        return index;
    }


    /**
     * Returns an iterator over the group value names
     * <p>{@link Iterator#remove()} is not supported</p>
     *
     * @return group value names iterator
     */
    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            int c = 0;

            @Override
            public boolean hasNext() {
                return c < headers.length - 2;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }

            @Override
            public String next() {
                return headers[c++];
            }
        };
    }


}
