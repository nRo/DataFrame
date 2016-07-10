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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 27.05.2016.
 */
public class SingleIndex implements Index {
    private final Map<Comparable, Integer> keyIndexMap = new HashMap<>();
    private final Map<Integer, Comparable> indexKeyMap = new HashMap<>();
    private final DataFrameColumn column;
    private final String name;

    /**
     * Creates a single index using  exactly one column
     *
     * @param indexName name of index
     * @param column    index column
     */
    protected SingleIndex(String indexName, DataFrameColumn column) {
        this.column = column;
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
    public void update(DataRow dataRow) {
        Comparable currentKey = indexKeyMap.get(dataRow.getIndex());
        if (currentKey != null) {
            keyIndexMap.remove(currentKey);
        }
        Comparable value = dataRow.get(column.getName());
        if (keyIndexMap.containsKey(value)) {
            throw new IllegalArgumentException("error adding row to index: duplicate values found '%s'");
        }
        keyIndexMap.put(value, dataRow.getIndex());
        indexKeyMap.put(dataRow.getIndex(), value);
    }

    @Override
    public boolean containsColumn(DataFrameColumn column) {
        return this.column == column;
    }

    @Override
    public List<DataFrameColumn> getColumns() {
        List<DataFrameColumn> columns = new ArrayList<>(1);
        columns.add(column);
        return columns;
    }

    @Override
    public void remove(DataRow dataRow) {
        Comparable value = dataRow.get(column.getName());
        if (keyIndexMap.containsKey(value)) {
            throw new IllegalArgumentException("error adding row to index: duplicate values found '%s'");
        }
        keyIndexMap.remove(value);
        indexKeyMap.remove(dataRow.getIndex());

    }

    @Override
    public int find(Comparable... values) {
        if (values.length != 1) {
            throw new IllegalArgumentException("only one value allowed");
        }
        Integer index = keyIndexMap.get(values[0]);
        return index == null ? -1 : index;
    }


}
