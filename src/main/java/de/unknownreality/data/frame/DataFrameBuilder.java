package de.unknownreality.data.frame;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.frame.column.DataColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameBuilder {
    private List<DataColumn> columns = new ArrayList<>();
    private DataContainer dataContainer;

    private DataFrameBuilder(DataContainer dataContainer){
        this.dataContainer = dataContainer;
    }
    public static DataFrameBuilder create(DataContainer dataContainer){
        return new DataFrameBuilder(dataContainer);
    }

    public DataFrameBuilder addColumn(DataColumn column){
        columns.add(column);
        return this;
    }


    public DataFrame build(){
        return DataFrameParser.fromCSV(dataContainer,columns);
    }







}
