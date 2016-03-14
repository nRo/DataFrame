package de.unknownreality.data.frame;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.Header;
import de.unknownreality.data.common.Row;
import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.csv.CSVRow;
import de.unknownreality.data.frame.column.DataColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameParser {
    private static Logger log = LoggerFactory.getLogger(DataFrameParser.class);

    public static DataFrame fromCSV(DataContainer<?super Header,? super Row> reader, List<DataColumn> columns){
        for(Row row : reader){
            for(DataColumn column : columns){
                String strVal = row.getString(column.getName());
                if(strVal == null || "".equals(strVal) || "null".equals(strVal)){
                    column.appendNA();
                    continue;
                }
                try{
                    if(Values.NA.isNA(strVal)){
                        column.appendNA();
                        continue;
                    }
                    Object o = column.getParser().parse(strVal);
                    if(o == null || !(o instanceof Comparable)){
                        column.appendNA();
                        continue;
                    }
                    column.append((Comparable)o);
                }
                catch (ParseException e){
                    log.warn("error parsing value, NA added",e);
                    column.appendNA();
                    continue;
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
