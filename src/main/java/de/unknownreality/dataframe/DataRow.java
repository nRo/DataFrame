package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.row.UpdateableRow;

/**
 * Created by Alex on 12.06.2017.
 */
public class DataRow extends UpdateableRow<String,DataFrameHeader,Comparable> {
    public DataRow(DataFrameHeader header, Comparable[] values, int index) {
        super(header,values,index);
    }

    /**
     * Checks whether a certain value is compatible for a column.
     * Compatible means that the value has a type suited for the respective class
     * @param value checked value
     * @param headerName header name
     * @return true if value is compatible
     */
    @Override
    public boolean isCompatible(Comparable value, String headerName) {
        Class<? extends Comparable> type = getHeader().getType(headerName);
        return type.isAssignableFrom(value.getClass());
    }

    /**
     * Checks whether a certain value is compatible for a column.
     * Compatible means that the value has a type suited for the respective class
     * @param value checked value
     * @param headerIndex header index
     * @return true if value is compatible
     */
    @Override
    public boolean isCompatible(Comparable value, int headerIndex) {
        Class<? extends Comparable> type = getHeader().getType(headerIndex);
        return type.isAssignableFrom(value.getClass());
    }

}
