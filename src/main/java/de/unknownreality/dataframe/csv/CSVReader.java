package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.mapping.*;

import de.unknownreality.dataframe.DataFrameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    private String[] ignorePrefixes;

    protected CSVReader(Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes ){
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
        this.ignorePrefixes = ignorePrefixes;
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

    public String[] getIgnorePrefixes() {
        return ignorePrefixes;
    }

    public void initHeader(){
        try{
            CSVIterator iterator = iterator();
            CSVRow row = iterator.next();
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
            iterator.close();

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
