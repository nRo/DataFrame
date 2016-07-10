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

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class Indices {
    private static Logger logger = LoggerFactory.getLogger(Indices.class);

    private final Map<String, Index> indexMap = new HashMap<>();
    private final Map<DataFrameColumn, List<Index>> columnIndexMap = new WeakHashMap<>();
    private final DataFrame dataFrame;

    /**
     * Creates an index for a data frame
     *
     * @param dataFrame input data frame
     */
    public Indices(DataFrame dataFrame) {
        this.dataFrame = dataFrame;
    }

    /**
     * Returns <tt>true</tt> if the specified column is part of at least one index.
     *
     * @param column input column
     * @return <tt>true</tt> if column is part of an index
     */
    public boolean isIndexColumn(DataFrameColumn column) {
        return columnIndexMap.containsKey(column);
    }

    /**
     * Copies all indices into another data frame
     *
     * @param dataFrame data frame the indices are copied to
     */
    public void copyTo(DataFrame dataFrame) {
        for (Map.Entry<String, Index> entry : indexMap.entrySet()) {
            List<DataFrameColumn> indexColumns = entry.getValue().getColumns();
            DataFrameColumn[] dfColumns = new DataFrameColumn[indexColumns.size()];
            boolean invalid = false;
            for (int i = 0; i < indexColumns.size(); i++) {
                DataFrameColumn dfCol = dataFrame.getColumn(indexColumns.get(i).getName());
                if (dfCol == null) {
                    invalid = true;
                    break;
                }
                dfColumns[i] = dfCol;
            }
            if (!invalid) {
                dataFrame.addIndex(entry.getKey(), dfColumns);
            }
        }
    }

    /**
     * Updates a data row in all available indices
     *
     * @param dataRow data row to update
     */
    public void update(DataRow dataRow) {
        for (Index index : indexMap.values()) {
            index.update(dataRow);
        }
    }

    /**
     * Returns the row number for a index and a set of values for the index
     *
     * @param name   name of the index
     * @param values row values
     * @return row number matching the row values
     */
    public int find(String name, Comparable... values) {
        if (!indexMap.containsKey(name)) {
            throw new IllegalArgumentException(String.format("index not found'%s'", name));
        }
        return indexMap.get(name).find(values);
    }

    /**
     * Creates and adds a new index using one or more columns
     *
     * @param name    name of the index
     * @param columns columns the index is based on
     */
    public void addIndex(String name, DataFrameColumn... columns) {
        if (indexMap.containsKey(name)) {
            throw new IllegalArgumentException(String.format("error adding index: index name already exists'%s'", name));
        }
        Index index;
        if (columns.length == 1) {
            index = new SingleIndex(name, columns[0]);
        } else {
            index = new MultiIndex(name, columns);
        }
        indexMap.put(name, index);
        for (DataFrameColumn column : columns) {
            List<Index> indexList = columnIndexMap.get(column);
            if (indexList == null) {
                indexList = new ArrayList<>();
                columnIndexMap.put(column, indexList);
            }
            indexList.add(index);
        }
        for (DataRow row : dataFrame) {
            index.update(row);
        }

    }

    /**
     * Returns <tt>true</tt> if an index with the specified name exists
     *
     * @param name name of index
     * @return <tt>true</tt> if index with input name exists
     */
    public boolean containsIndex(String name) {
        return indexMap.containsKey(name);
    }

    /**
     * Clears the values of all indices
     */
    public void clearValues() {
        for (Index index : indexMap.values()) {
            index.clear();
        }
    }


    /**
     * Updates the specified in each index that contains the specified column
     *
     * @param column  index column
     * @param dataRow row to update
     */
    public void updateValue(DataFrameColumn column, DataRow dataRow) {
        if (!isIndexColumn(column)) {
            return;
        }
        for (Index indexObject : columnIndexMap.get(column)) {
            indexObject.update(dataRow);
        }
    }

    /**
     * Updates all indices that contain a certain row.
     * All rows in the data frame are updated
     *
     * @param column update column
     */
    public void updateColumn(DataFrameColumn column) {
        if (!isIndexColumn(column)) {
            return;
        }
        Collection<Index> columnIndices = columnIndexMap.get(column);
        for (Index indexObject : columnIndices) {
            indexObject.clear();
        }
        for (DataRow row : dataFrame) {
            for (Index indexObject : columnIndices) {
                indexObject.update(row);
            }
        }
    }

    /**
     * Removes a row from all indices
     *
     * @param dataRow row to remove
     */
    public void remove(DataRow dataRow) {
        for (Index index : indexMap.values()) {
            index.remove(dataRow);
        }
    }

    /**
     * Removes an index based on its name
     *
     * @param name name of index to remove
     */
    public void removeIndex(String name) {
        Index index = indexMap.get(name);
        if (index == null) {
            return;
        }
        indexMap.remove(name);
        for (DataFrameColumn column : index.getColumns()) {
            List<Index> colIndices = columnIndexMap.get(column);
            colIndices.remove(index);
            if (colIndices.isEmpty()) {
                columnIndexMap.remove(column);
            }
        }
    }

    /**
     * Removes all indices that contain a certain column
     *
     * @param column column to remove
     */
    public void removeColumn(DataFrameColumn column) {
        if (!isIndexColumn(column)) {
            return;
        }
        List<Index> columnIndices = columnIndexMap.get(column);
        for (Index index : columnIndices) {
            removeIndex(index.getName());
        }
        columnIndexMap.remove(column);
    }


}
