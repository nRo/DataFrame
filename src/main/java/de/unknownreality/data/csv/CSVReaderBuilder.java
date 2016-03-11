package de.unknownreality.data.csv;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVReaderBuilder {
    private File file;
    private String separator = "\t";
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private CSVReaderBuilder(File file){
        this.file = file;
    }
    public static CSVReaderBuilder create(File file){
        return new CSVReaderBuilder(file);
    }


    public CSVReaderBuilder withSeparator(String separator){
        this.separator = separator;
        return this;
    }

    public CSVReaderBuilder containsHeader(boolean containsHeader){
        this.containsHeader = containsHeader;
        return this;
    }

    public CSVReaderBuilder withHeaderPrefix(String headerPrefix){
        this.headerPrefix = headerPrefix;
        return this;
    }

    public CSVReader build(){
        return new CSVReader(file,separator,containsHeader,headerPrefix);
    }
}
