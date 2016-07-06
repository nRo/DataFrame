package de.unknownreality.dataframe.frame.sort;

import de.unknownreality.dataframe.frame.DataFrameHeader;
import de.unknownreality.dataframe.frame.DataRow;

import java.util.Comparator;

/**
 * Created by Alex on 09.03.2016.
 */
public class RowColumnComparator implements Comparator<DataRow> {
    private SortColumn[] sortColumns;
    private DataFrameHeader header;
    public RowColumnComparator(DataFrameHeader header,SortColumn[] sortColumns){
        this.sortColumns = sortColumns;
        this.header = header;
    }

    @Override
    public int compare(DataRow r1, DataRow r2) {
        int c = 0;
        for(SortColumn sortColumn : sortColumns){
            String name = sortColumn.getName();
            if(r1.isNA(name) && r2.isNA(name)){
                c = 0;
                continue;
            }
            if(r1.isNA(name)){
                return 1;
            }
            if(r2.isNA(name)){
                return -1;
            }
            Comparable a = r1.get(sortColumn.getName());
            Comparable b = r2.get(sortColumn.getName());
            c =  a.compareTo(b);
            c = sortColumn.getDirection() == SortColumn.Direction.Ascending ? c : -c;
            if(c != 0){
                return c;
            }
        }
        return c;
    }
}
