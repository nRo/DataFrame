package de.unknownreality.data.csv;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.mapping.*;

import de.unknownreality.data.frame.DataFrameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class CSVReader implements DataContainer<CSVHeader,CSVRow>{
    private static Logger log = LoggerFactory.getLogger(CSVReader.class);
    private String headerPrefix = "#";
    private Character separator;
    private File file;
    private CSVHeader header = new CSVHeader();
    private boolean containsHeader;

    protected CSVReader(Character separator, boolean containsHeader,String headerPrefix){
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
    }

    public boolean containsHeader() {
        return containsHeader;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public Character getSeparator() {
        return separator;
    }

    public CSVHeader getHeader() {
        if(header == null){
            initHeader();
        }
        return header;
    }

    public void initHeader(){
        try{
            CSVIterator iterator = iterator();
            CSVRow row = iterator.getFirstRow();
            iterator.close();
            if(row == null){
                containsHeader = false;
                return;
            }
            if(containsHeader){
                if(!row.get(0).startsWith(headerPrefix)){
                    throw new CSVException("invalid header prefix in first line");
                }
                for(int i = 0; i < row.size();i++){
                    String name = row.get(i);
                    if(i == 0){
                        name = headerPrefix == null ? name : name.substring(headerPrefix.length());
                    }
                    header.add(name);
                }
            }
            else{
                for(int i = 0; i < row.size();i++){
                    header.add();
                }
                containsHeader = false;
            }
        }  catch (CSVException e) {
            log.error("error reading file: {}",file,e);
        }
    }

    @Override
    public abstract CSVIterator iterator();

    public DataFrameBuilder toDataFrame(){
        return DataFrameBuilder.create(this);
    }
    public <T> List<T> map(Class<T> cl){
        return DataMapper.map(this,cl);
    }


}
