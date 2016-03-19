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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameParser {
    private static Logger log = LoggerFactory.getLogger(DataFrameParser.class);

    public static DataFrame fromCSV(DataContainer<?super Header,? super Row> reader, LinkedHashMap<String,DataColumn> columns){
        Map<String,Object> parserCache = new HashMap<>();
        for(Row row : reader){
            for(Map.Entry<String,DataColumn> columnEntry : columns.entrySet()){
                String strVal = row.getString(columnEntry.getKey());
                if(strVal == null || "".equals(strVal) || "null".equals(strVal)){
                    columnEntry.getValue().appendNA();
                    continue;
                }
                try{
                    if(Values.NA.isNA(strVal)){
                        columnEntry.getValue().appendNA();
                        continue;
                    }
                    Object o;
                    String object_ident = columnEntry.getValue().getType()+"::"+strVal;
                    if((o = parserCache.get(object_ident)) == null){
                        o = columnEntry.getValue().getParser().parse(strVal);
                        parserCache.put(object_ident,o);
                    }
                    if(o == null || !(o instanceof Comparable)){
                        columnEntry.getValue().appendNA();
                        continue;
                    }
                    columnEntry.getValue().append((Comparable)o);
                }
                catch (ParseException e){
                    log.warn("error parsing value, NA added",e);
                    columnEntry.getValue().appendNA();
                    continue;
                }

            }
        }
        DataFrame dataFrame = new DataFrame();
        for(DataColumn column : columns.values()){
            dataFrame.addColumn(column);
        }
        return dataFrame;
    }
}
