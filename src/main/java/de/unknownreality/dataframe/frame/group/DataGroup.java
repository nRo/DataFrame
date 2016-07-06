package de.unknownreality.dataframe.frame.group;

import de.unknownreality.dataframe.frame.DataFrame;

/**
 * Created by Alex on 10.03.2016.
 */
public class DataGroup extends DataFrame{
    private GroupHeader groupHeader;
    private GroupValues groupValues;
    public DataGroup(String[] columns,Comparable[] values){
        if(columns.length != values.length){
            throw new IllegalArgumentException("column and values must have same length");
        }
        groupHeader = new GroupHeader(columns);
        Comparable[] gvals = new Comparable[values.length];
        System.arraycopy(values,0,gvals,0,values.length);
        this.groupValues = new GroupValues(gvals,groupHeader);
    }


    public GroupHeader getGroupHeader() {
        return groupHeader;
    }

    public GroupValues getGroupValues() {
        return groupValues;
    }

    public String getGroupDescription(){
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for(String h : groupHeader){
            sb.append(h).append("=").append(groupValues.get(h));
            if(i++ < groupHeader.size() - 1){
                sb.append(", ");
            }
        }
        return sb.toString();
    }


}



