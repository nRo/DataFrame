package de.unknownreality.data.frame;

import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.frame.column.DataColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameBuilder {
    private List<DataColumn> columns = new ArrayList<>();
    private CSVReader csvReader;

    private DataFrameBuilder(CSVReader csvReader){
        this.csvReader = csvReader;
    }
    public static DataFrameBuilder create(CSVReader csvReader){
        return new DataFrameBuilder(csvReader);
    }

    public DataFrameBuilder addColumn(DataColumn column){
        columns.add(column);
        return this;
    }


    public DataFrame build(){
        return DataFrameParser.fromCSV(csvReader,columns);
    }







}
