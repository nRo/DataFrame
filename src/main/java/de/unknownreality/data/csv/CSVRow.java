package de.unknownreality.data.csv;

import de.unknownreality.data.common.Row;
import de.unknownreality.data.common.parser.ParserNotFoundException;
import de.unknownreality.data.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Iterator;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVRow implements Iterable<String>,Row<String> {
    private static Logger log = LoggerFactory.getLogger(CSVRow.class);

    private String[] values;
    private String separator;
    private CSVHeader header;
    private int rowNumber;
    public CSVRow(CSVHeader header,String[] values,int rowNumber, String separator){
        this.values = values;
        this.separator = separator;
        this.header = header;
        this.rowNumber = rowNumber;
    }

    public int getRowNumber() {
        return rowNumber;
    }

    public String[] getValues() {
        return values;
    }

    public String get(int index){
        if(index >= values.length){
            throw new IllegalArgumentException(String.format("header index out of bounds %d > %d",index,(values.length-1)));
        }
        return values[index];
    }

    public String get(String headerName){
        int index = header.getIndex(headerName);
        if(index == -1){
            throw new IllegalArgumentException(String.format("header name not found '%s'",headerName));
        }
        return get(index);
    }

    @Override
    public String getString(int index) {
        return get(index);
    }

    @Override
    public String getString(String headerName) {
        return get(headerName);
    }

    public boolean getBooleanValue(int index){
        String val = get(index);
        try {
            return ParserUtil.parse(Boolean.class,values[index]);
        } catch (ParseException e) {
            log.error("error parsing value {} to {}",val,Boolean.class,e);
        } catch (ParserNotFoundException e) {
            log.error("error parsing value {} to {}",val,Boolean.class,e);
        }
        return false;
    }


    public Double getDouble(int index){
        return getValueAsOrNull(get(index),Double.class);
    }
    public Double getDouble(String headerName){
        return getValueAsOrNull(get(headerName),Double.class);
    }

    public Integer getInteger(int index){
        return getValueAsOrNull(get(index),Integer.class);
    }
    public Integer getInteger(String headerName){
        return getValueAsOrNull(get(headerName),Integer.class);
    }

    public Long getLong(int index){
        return getValueAsOrNull(get(index),Long.class);
    }
    public Long getLong(String headerName){
        return getValueAsOrNull(get(headerName),Long.class);
    }
    public Boolean getBoolean(int index){
        return getValueAsOrNull(get(index),Boolean.class);
    }
    public Boolean getBoolean(String headerName){
        return getValueAsOrNull(get(headerName),Boolean.class);
    }
    public Float getFloat(int index){
        return getValueAsOrNull(get(index),Float.class);
    }
    public Float getFloat(String headerName){
        return getValueAsOrNull(get(headerName),Float.class);
    }

    public Character getChar(int index){
        return getValueAsOrNull(get(index),Character.class);
    }
    public Character getChar(String headerName){
        return getValueAsOrNull(get(headerName),Character.class);
    }

    @Override
    public<T> T get(String headerName, Class<T> cl) {
        return getValueAsOrNull(get(headerName),cl);
    }
    private  <T> T getValueAsOrNull(String value, Class<T> cl){
        try {
            return ParserUtil.parse(cl,value);
        } catch (ParseException e) {
            log.error("error parsing value {} to {}",value,cl,e);
        } catch (ParserNotFoundException e) {
            log.error("error parsing value {} to {}",value,cl,e);
        }
        return null;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < values.length - 1;
            }

            @Override
            public String next() {
                return values[index++];
            }
        };
    }

    @Override
    public String toString(){
        return String.join(separator, values);
    }
}
