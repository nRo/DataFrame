package de.unknownreality.dataframe.frame.group;

import de.unknownreality.dataframe.frame.DataFrame;
import de.unknownreality.dataframe.frame.DataRow;
import de.unknownreality.dataframe.frame.sort.SortColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 10.03.2016.
 */
public class DataFrameGroupUtil {
    public static DataGrouping groupBy(DataFrame df, String... columns) {
        SortColumn[] sortColumns = new SortColumn[columns.length];
        for(int i = 0; i < columns.length;i++){
            sortColumns[i] = new SortColumn(columns[i]);
        }
        DataFrame sortedFrame = df.copy().sort(sortColumns);
        List<DataRow> currentList = new ArrayList<>();
        Comparable[] lastValues = null;
        List<DataGroup> groupList = new ArrayList<>();
        for (DataRow row : sortedFrame) {
            if (lastValues == null || equals(lastValues,row,columns)) {
                currentList.add(row);
                if(lastValues == null){
                    lastValues = new Comparable[columns.length];
                    set(lastValues,row,columns);
                }
                continue;
            }
            if (!currentList.isEmpty()) {
                DataGroup group = new DataGroup(columns,lastValues);
                group.set(df.getHeader().copy(), currentList);
                groupList.add(group);
            }
            currentList.clear();
            currentList.add(row);
            set(lastValues,row,columns);
        }
        if (!currentList.isEmpty()) {
            DataGroup group = new DataGroup(columns,lastValues);
            group.set(df.getHeader().copy(), currentList);
            groupList.add(group);
        }
        return new DataGrouping(groupList,columns);
    }

    private static boolean equals(Object[] values, DataRow row, String[] columns){
        for(int i = 0; i < values.length;i++){
            if(values[i] == null && !row.isNA(i)){
                return false;
            }
            if(values[i] != null && row.isNA(i)){
                return false;
            }
            if(values[i] == null && row.isNA(i)){
                continue;
            }
            if(!values[i].equals(row.get(columns[i]))){
                return false;
            }
        }
        return true;
    }

    private static boolean set(Object[] values, DataRow row, String[] columns){
        for(int i = 0; i < values.length;i++){
            values[i] = row.get(columns[i]);
        }
        return true;
    }
}
