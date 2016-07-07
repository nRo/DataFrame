package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DataFrameColumn;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class SingleIndex implements Index {
    private final Map<Comparable, Integer> keyIndexMap = new HashMap<>();
    private final Map<Integer, Comparable> indexKeyMap = new HashMap<>();
    private final DataFrameColumn column;
    private final String name;

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
