package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DataFrameColumn;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class MultiIndex implements Index {
    private Map<MultiKey,Integer> keyIndexMap = new HashMap<>();
    private Map<Integer,MultiKey> indexKeyMap = new HashMap<>();

    private Map<DataFrameColumn,Integer> columnIndexMap = new LinkedHashMap<>();
    private String name;
    protected MultiIndex(String indexName,DataFrameColumn... columns) {
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
    public void update(DataRow dataRow){
        MultiKey currentKey = indexKeyMap.get(dataRow.getIndex());
        if(currentKey != null){
            keyIndexMap.remove(currentKey);
            int i = 0;
            for(DataFrameColumn column : columnIndexMap.keySet()){
                currentKey.getValues()[i++] = dataRow.get(column.getName());
            }
            currentKey.updateHash();
        }
        else{
            Comparable[] values = new Comparable[columnIndexMap.size()];
            int i = 0;
            for(DataFrameColumn column : columnIndexMap.keySet()){
                values[i++] = dataRow.get(column.getName());
            }
            currentKey = new MultiKey(values);
        }

        if(keyIndexMap.containsKey(currentKey)){
            throw new IllegalArgumentException(String.format("error adding row to index: duplicated values found '%s'",currentKey));
        }
        keyIndexMap.put(currentKey,dataRow.getIndex());
        indexKeyMap.put(dataRow.getIndex(),currentKey);
    }

    @Override
    public void remove(DataRow dataRow){
        Comparable[] values = new Comparable[columnIndexMap.size()];
        int i = 0;
        for(DataFrameColumn column : columnIndexMap.keySet()){
            values[i++] = dataRow.get(column.getName());
        }
        keyIndexMap.remove(new MultiKey(values));
        indexKeyMap.remove(dataRow.getIndex());
    }

    @Override
    public int find(Comparable... values){
        if(values.length != columnIndexMap.size()){
            throw new IllegalArgumentException(String.format("value for each index column required"));
        }
        Integer index = keyIndexMap.get(new MultiKey(values));
        return index == null ? -1 : index;
    }

}
