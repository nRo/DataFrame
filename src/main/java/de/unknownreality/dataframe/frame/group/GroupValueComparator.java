package de.unknownreality.dataframe.frame.group;

import de.unknownreality.dataframe.frame.Values;
import de.unknownreality.dataframe.frame.sort.SortColumn;

import java.util.Comparator;

/**
 * Created by Alex on 09.03.2016.
 */
public class GroupValueComparator implements Comparator<DataGroup> {
    private SortColumn[] sortColumns;
    public GroupValueComparator(SortColumn[] sortColumns){
        this.sortColumns = sortColumns;
    }

    @Override
    public int compare(DataGroup r1, DataGroup r2) {
        int c = 0;
        for(SortColumn sortColumn : sortColumns){
            Comparable r1Val = r1.getGroupValues().get(sortColumn.getName());
            Comparable r2Val = r2.getGroupValues().get(sortColumn.getName());
            boolean r1NA = r1Val == null || r1Val == Values.NA;
            boolean r2NA = r2Val == null || r2Val == Values.NA;

            if(r1NA && r2NA ){
                c = 0;
                continue;
            }
            if(r1NA ){
                return 1;
            }
            if(r2NA){
                return -1;
            }
            c =  r1Val.compareTo(r2Val);
            c = sortColumn.getDirection() == SortColumn.Direction.Ascending ? c : -c;
            if(c != 0){
                return c;
            }
        }
        return c;
    }
}
