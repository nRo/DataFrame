package de.unknownreality.data.frame.sort;

import de.unknownreality.data.frame.DataFrameHeader;
import de.unknownreality.data.frame.DataRow;

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
