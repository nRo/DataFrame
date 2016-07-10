/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.common.MultiKey;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class MultiIndex implements Index {
    private final Map<MultiKey, Integer> keyIndexMap = new HashMap<>();
    private final Map<Integer, MultiKey> indexKeyMap = new HashMap<>();

    private final Map<DataFrameColumn, Integer> columnIndexMap = new LinkedHashMap<>();
    private final String name;

    /**
     * Creates a multi index using more that one column
     *
     * @param indexName name of index
     * @param columns   index columns
     */
    protected MultiIndex(String indexName, DataFrameColumn... columns) {
        int i = 0;
        for (DataFrameColumn column : columns) {
            columnIndexMap.put(column, i++);
        }
        this.name = indexName;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public void clear() {
        keyIndexMap.clear();
        indexKeyMap.clear();
    }

    @Override
    public boolean containsColumn(DataFrameColumn column) {
        return columnIndexMap.containsKey(column);
    }

    @Override
    public List<DataFrameColumn> getColumns() {
        return new ArrayList<>(columnIndexMap.keySet());
    }

    @Override
    public void update(DataRow dataRow) {
        MultiKey currentKey = indexKeyMap.get(dataRow.getIndex());
        if (currentKey != null) {
            keyIndexMap.remove(currentKey);
            int i = 0;
            for (DataFrameColumn column : columnIndexMap.keySet()) {
                currentKey.getValues()[i++] = dataRow.get(column.getName());
            }
            currentKey.updateHash();
        } else {
            Comparable[] values = new Comparable[columnIndexMap.size()];
            int i = 0;
            for (DataFrameColumn column : columnIndexMap.keySet()) {
                values[i++] = dataRow.get(column.getName());
            }
            currentKey = new MultiKey(values);
        }

        if (keyIndexMap.containsKey(currentKey)) {
            throw new IllegalArgumentException(String.format("error adding row to index: duplicated values found '%s'", currentKey));
        }
        keyIndexMap.put(currentKey, dataRow.getIndex());
        indexKeyMap.put(dataRow.getIndex(), currentKey);
    }

    @Override
    public void remove(DataRow dataRow) {
        Comparable[] values = new Comparable[columnIndexMap.size()];
        int i = 0;
        for (DataFrameColumn column : columnIndexMap.keySet()) {
            values[i++] = dataRow.get(column.getName());
        }
        keyIndexMap.remove(new MultiKey(values));
        indexKeyMap.remove(dataRow.getIndex());
    }

    @Override
    public int find(Comparable... values) {
        if (values.length != columnIndexMap.size()) {
            throw new IllegalArgumentException("value for each index column required");
        }
        Integer index = keyIndexMap.get(new MultiKey(values));
        return index == null ? -1 : index;
    }

}
