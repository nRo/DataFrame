package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.common.DataContainer;

import java.util.LinkedHashMap;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameBuilder {
    private LinkedHashMap<String,DataFrameColumn> columns = new LinkedHashMap<>();
    private DataContainer dataContainer;
    private DataFrameBuilder(DataContainer dataContainer){
        this.dataContainer = dataContainer;
    }
    public static DataFrameBuilder create(DataContainer dataContainer){
        return new DataFrameBuilder(dataContainer);
    }

    public DataFrameBuilder addColumn(DataFrameColumn column){
        columns.put(column.getName(),column);
        return this;
    }

    public DataFrameBuilder addColumn(String header,DataFrameColumn column){
        columns.put(header,column);
        return this;
    }


    public DataFrame build(){
        return DataFrameConverter.fromDataContainer(dataContainer, columns);

    }







}
