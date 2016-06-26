package de.unknownreality.data.frame.index;

import de.unknownreality.data.frame.DataFrame;
import de.unknownreality.data.frame.DataRow;
import de.unknownreality.data.frame.DataFrameColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class Indices {
    private static Logger logger = LoggerFactory.getLogger(Indices.class);

    private Map<String,Index> indexMap = new HashMap<>();
    private Map<DataFrameColumn,List<Index>> columnIndexMap = new WeakHashMap<>();
    private DataFrame dataFrame;
    public Indices(DataFrame dataFrame){
        this.dataFrame = dataFrame;
    }
    public boolean isIndexColumn(DataFrameColumn column){
        return columnIndexMap.containsKey(column);
    }

    public void copyTo(DataFrame dataFrame){
        for(Map.Entry<String,Index> entry : indexMap.entrySet()){
            List<DataFrameColumn> indexColumns = entry.getValue().getColumns();
            DataFrameColumn[] dfColumns = new DataFrameColumn[indexColumns.size()];
            boolean invalid = false;
            for(int i = 0 ; i < indexColumns.size();i++){
                DataFrameColumn dfCol =  dataFrame.getColumn(indexColumns.get(i).getName());
                if(dfCol == null){
                    invalid = true;
                    break;
                }
                dfColumns[i] = dfCol;
            }
            if(!invalid){
                dataFrame.addIndex(entry.getKey(),dfColumns);
            }
        }
    }

    public void update(DataRow dataRow){
        for(Index index : indexMap.values()){
            index.update(dataRow);
        }
    }

    public int find(String name,Comparable... values){
        if(!indexMap.containsKey(name)){
            throw new IllegalArgumentException(String.format("index not found'%s'",name));
        }
        return indexMap.get(name).find(values);
    }

    public void addIndex(String name, DataFrameColumn... columns){
        if(indexMap.containsKey(name)){
            throw new IllegalArgumentException(String.format("error adding index: index name already exists'%s'",name));
        }
        Index index;
        if(columns.length == 1){
            index = new SingleIndex(name,columns[0]);
        }
        else{
            index = new MultiIndex(name, columns);
        }
        indexMap.put(name,index);
        for(DataFrameColumn column : columns){
            List<Index> indexList = columnIndexMap.get(column);
            if(indexList == null){
                indexList = new ArrayList<>();
                columnIndexMap.put(column,indexList);
            }
            indexList.add(index);
        }
        for(DataRow row : dataFrame){
            index.update(row);
        }

    }

    public boolean containsIndex(String name){
        return indexMap.containsKey(name);
    }

    public void clearValues(){
        for(Index index : indexMap.values()){
            index.clear();
        }
    }

    public void updateValue(DataFrameColumn column, DataRow dataRow){
        if(!isIndexColumn(column)){
            return;
        }
        for(Index indexObject : columnIndexMap.get(column)){
            indexObject.update(dataRow);
        }
    }

    public void updateColumn(DataFrameColumn column){
        if(!isIndexColumn(column)){
            return;
        }
        Collection<Index> columnIndices = columnIndexMap.get(column);
        for(Index indexObject : columnIndices){
            indexObject.clear();
        }
        for(DataRow row : dataFrame){
            for(Index indexObject : columnIndices){
                indexObject.update(row);
            }
        }
    }

    public void remove(DataRow dataRow){
        for(Index index : indexMap.values()){
            index.remove(dataRow);
        }
    }

    public void removeIndex(String name){
        Index index = indexMap.get(name);
        if(index == null){
            return;
        }
        indexMap.remove(name);
        for(DataFrameColumn column : index.getColumns()){
            List<Index> colIndices = columnIndexMap.get(column);
            colIndices.remove(index);
            if(colIndices.isEmpty()){
                columnIndexMap.remove(column);
            }
        }
    }

    public void removeColumn(DataFrameColumn column){
        if(!isIndexColumn(column)){
            return;
        }
        List<Index> columnIndices = columnIndexMap.get(column);
        for(Index index : columnIndices){
            removeIndex(index.getName());
        }
        columnIndexMap.remove(column);
    }


}
