package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.header.BasicTypeHeader;

/**
 * Created by algru on 11.06.2017.
 */
public class DataFrameHeader extends BasicTypeHeader<String> {




    public DataFrameHeader add(DataFrameColumn<?, ?> column) {
        return (DataFrameHeader)add(column.getName(), column.getClass(), column.getType());
    }

    public DataFrameHeader replace(DataFrameColumn<?, ?> existing, DataFrameColumn<?, ?> replacement){
        return (DataFrameHeader) replace(existing.getName(),replacement.getName(),replacement.getClass(), replacement.getType());
    }
    public DataFrameHeader copy() {
        DataFrameHeader copy = new DataFrameHeader();
        for (String h : this) {
            copy.add(h, getColumnType(h), getType(h));
        }
        return copy;
    }
}
