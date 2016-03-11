package de.unknownreality.data.frame;

import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.csv.CSVRow;
import de.unknownreality.data.frame.column.DataColumn;

import java.text.ParseException;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameParser {


    public static DataFrame fromCSV(CSVReader reader, List<DataColumn> columns){
        for(CSVRow row : reader){
            for(DataColumn column : columns){
                String strVal = row.getString(column.getName());
                try{
                    Object o = column.getParser().parse(strVal);
                    if(o == null || !(o instanceof Comparable)){
                        //ERROR
                    }
                    column.append((Comparable)o);
                }
                catch (ParseException e){
                    //error
                }

            }
        }
        DataFrame dataFrame = new DataFrame();
        for(DataColumn column : columns){
            dataFrame.addColumn(column);
        }
        return dataFrame;
    }
}
