package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserNotFoundException;
import de.unknownreality.dataframe.common.parser.ParserUtil;
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
    private Character separator;
    private CSVHeader header;
    private int rowNumber;
    public CSVRow(CSVHeader header, String[] values, int rowNumber, Character separator){
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
        return values[index];
    }

    @Override
    public String getString(int index) {
        return get(index);
    }

    @Override
    public String getString(String headerName) {
        return get(headerName);
    }

    private static Parser<Boolean> BOOLEAN_PARSER = ParserUtil.findParserOrNull(Boolean.class);
    private static Parser<Double> DOUBLE_PARSER = ParserUtil.findParserOrNull(Double.class);
    private static Parser<Float> FLOAT_PARSER = ParserUtil.findParserOrNull(Float.class);
    private static Parser<Long> LONG_PARSER = ParserUtil.findParserOrNull(Long.class);
    private static Parser<Integer> INTEGER_PARSER = ParserUtil.findParserOrNull(Integer.class);


    public Boolean getBoolean(int index){
        return parse(index,Boolean.class,BOOLEAN_PARSER);

    }

    public Boolean getBoolean(String header){
        return parse(header,Boolean.class,BOOLEAN_PARSER);

    }

    public Double getDouble(int index){
        return parse(index,Double.class,DOUBLE_PARSER);

    }

    public Double getDouble(String header){
        return parse(header,Double.class,DOUBLE_PARSER);
    }


    public Long getLong(int index){
        return parse(index,Long.class,LONG_PARSER);
    }

    public Long getLong(String header){
        return parse(header,Long.class,LONG_PARSER);
    }
    public Integer getInteger(int index){
        return parse(index,Integer.class,INTEGER_PARSER);
    }

    public Integer getInteger(String header){
        return parse(header,Integer.class,INTEGER_PARSER);
    }
    public Float getFloat(int index){
        return parse(index,Float.class,FLOAT_PARSER);
    }

    public Float getFloat(String header){
        return parse(header,Float.class,FLOAT_PARSER);
    }



    private <T> T parse(String name,Class<T> cl,Parser<T> parser){
        String val = get(name);
        try {
            return parser.parse(val);
        } catch (ParseException e) {
            log.error("error parsing value {} to {}",val,cl,e);
        }
        return null;
    }
    private <T> T parse(int index,Class<T> cl,Parser<T> parser){
        String val = get(index);
        try {
            return parser.parse(val);
        } catch (ParseException e) {
            log.error("error parsing value {} to {}",val,cl,e);
        }
        return null;
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

            @Override
            public void remove(){};
        };
    }

    @Override
    public String toString(){
        return String.join(separator.toString(), values);
    }
}
