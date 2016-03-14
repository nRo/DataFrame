package de.unknownreality.data.frame;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.frame.column.DataColumn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameBuilder {
    private LinkedHashMap<String,DataColumn> columns = new LinkedHashMap<>();
    private DataContainer dataContainer;

    private DataFrameBuilder(DataContainer dataContainer){
        this.dataContainer = dataContainer;
    }
    public static DataFrameBuilder create(DataContainer dataContainer){
        return new DataFrameBuilder(dataContainer);
    }

    public DataFrameBuilder addColumn(DataColumn column){
        columns.put(column.getName(),column);
        return this;
    }

    public DataFrameBuilder addColumn(String header,DataColumn column){
        columns.put(header,column);
        return this;
    }


    public DataFrame build(){
        return DataFrameParser.fromCSV(dataContainer,columns);
    }







}
