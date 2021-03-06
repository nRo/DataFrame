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

package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataRow;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class Indices {
    public static final String PRIMARY_KEY_NAME = "%primary_key%";

    private final Map<String, Index> indexMap = new HashMap<>();
    private final Map<DataFrameColumn<?, ?>, List<Index>> columnIndexMap = new WeakHashMap<>();
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
    public boolean isIndexColumn(DataFrameColumn<?, ?> column) {
        return columnIndexMap.containsKey(column);
    }

    /**
     * Replaces an existing column with a replacement column
     * All existing indices will be updated
     *
     * @param existing    existing column
     * @param replacement replacement column
     */
    public void replace(DataFrameColumn<?, ?> existing, DataFrameColumn<?, ?> replacement) {
        if (!isIndexColumn(existing)) {
            return;
        }
        List<Index> existingIndices = columnIndexMap.get(existing);
        for (Index idx : existingIndices) {
            idx.replaceColumn(existing, replacement);
            idx.clear();
        }
        columnIndexMap.remove(existing);
        columnIndexMap.put(replacement, existingIndices);
        for (DataRow row : dataFrame) {
            for (Index idx : existingIndices) {
                idx.update(row);
            }
        }
    }

    /**
     * Copies all indices into another data frame
     *
     * @param dataFrame data frame the indices are copied to
     */
    public void copyTo(DataFrame dataFrame) {
        for (Map.Entry<String, Index> entry : indexMap.entrySet()) {
            List<DataFrameColumn<?, ?>> indexColumns = entry.getValue().getColumns();
            DataFrameColumn<?, ?>[] dfColumns = new DataFrameColumn[indexColumns.size()];
            boolean invalid = false;
            for (int i = 0; i < indexColumns.size(); i++) {
                DataFrameColumn<?, ?> dfCol = dataFrame.getColumn(indexColumns.get(i).getName());
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
        if(indicesCount() == 0){
            return;
        }
        for (Index index : indexMap.values()) {
            index.update(dataRow);
        }
    }

    /**
     * Updates all rows in all available indices
     */
    public void updateAllRows(){
        if(indicesCount() == 0){
            return;
        }
        for(DataRow row : dataFrame){
            update(row);
        }
    }

    /**
     * Returns the row numbers for a index and a set of values for the index
     *
     * @param name   name of the index
     * @param values row values
     * @return row numbers matching the row values
     */
    public Collection<Integer> find(String name, Object... values) {
        if (!indexMap.containsKey(name)) {
            throw new DataFrameRuntimeException(String.format("index not found'%s'", name));
        }
        return indexMap.get(name).find(values);
    }

    /**
     * Returns the first found row number for a index and a set of values for the index
     * If no row is found, null is returned
     *
     * @param name   name of the index
     * @param values row values
     * @return row number matching the row values or null
     */
    public Integer findFirst(String name, Object... values) {
        Collection<Integer> rows = find(name, values);
        return rows.isEmpty() ? null : rows.iterator().next();
    }

    /**
     * Returns the row number for a set of primary key values
     *
     * @param values primary key values
     * @return row number matching the row values
     */
    public Integer findByPrimaryKey(Object... values) {
        Index primaryKey = indexMap.get(PRIMARY_KEY_NAME);
        if (primaryKey == null) {
            throw new DataFrameRuntimeException("no primaryKey found");
        }
        Collection<Integer> indices = primaryKey.find(values);
        if (indices.isEmpty()) {
            return null;
        }
        return indices.iterator().next();
    }

    /**
     * Creates and adds a new index using one or more columns
     *
     * @param name    name of the index
     * @param columns columns the index is based on
     */
    public void addIndex(String name, DataFrameColumn<?, ?>... columns) {
        addIndex(name, false, columns);
    }

    /**
     * Creates and adds a new index using one or more columns
     *
     * @param name    name of the index
     * @param unique  allow only unique values in this index
     * @param columns columns the index is based on
     */
    public void addIndex(String name, boolean unique, DataFrameColumn<?, ?>... columns) {

        Index index = new TreeIndex(name, columns);
        index.setUnique(unique);
        addIndex(index);
    }

    /**
     * Adds a new index using one or more columns
     *
     * @param index index to add
     */
    public void addIndex(Index index) {
        if (indexMap.containsKey(index.getName())) {
            throw new DataFrameRuntimeException(String.format("error adding index: index name already exists'%s'", index.getName()));
        }
        indexMap.put(index.getName(), index);
        for (DataFrameColumn<?, ?> column : index.getColumns()) {
            List<Index> indexList = columnIndexMap.computeIfAbsent(column, k -> new ArrayList<>());
            indexList.add(index);
        }
        for (DataRow row : dataFrame) {
            index.update(row);
        }
    }

    /**
     * returns the number of indices
     * @return number of indices
     */
    public int indicesCount(){
        return indexMap.size();
    }

    /**
     * sets the primary key using one or more columns
     *
     * @param columns columns the index is based on
     */
    public void setPrimaryKey(DataFrameColumn<?, ?>... columns) {
        if (indexMap.containsKey(PRIMARY_KEY_NAME)) {
            removeIndex(PRIMARY_KEY_NAME);
        }
        addIndex(PRIMARY_KEY_NAME, true, columns);
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
    public void updateValue(DataFrameColumn<?, ?> column, DataRow dataRow) {
        if (indicesCount() == 0) {
            return;
        }
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
    public void updateColumn(DataFrameColumn<?, ?> column) {
        if (indicesCount() == 0) {
            return;
        }
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
        if(indicesCount() == 0){
            return;
        }
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
        for (DataFrameColumn<?, ?> column : index.getColumns()) {
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
    public void removeColumn(DataFrameColumn<?, ?> column) {
        if (indicesCount() == 0) {
            return;
        }

        if (!isIndexColumn(column)) {
            return;
        }
        List<Index> columnIndices = new ArrayList<>(columnIndexMap.get(column));
        for (Index index : columnIndices) {
            removeIndex(index.getName());
        }
        columnIndexMap.remove(column);
    }

    public void clear(){
        this.columnIndexMap.clear();
        this.indexMap.clear();
    }

}
