package de.unknownreality.data.csv;

import de.unknownreality.data.frame.DataFrame;
import de.unknownreality.data.util.MultiIterator;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVMultiReaderBuilder {
    private File[] files;
    private String separator = "\t";
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private CSVMultiReaderBuilder(File[] files){
        this.files = files;
    }

    public static CSVMultiReaderBuilder create(File[] files){
        return new CSVMultiReaderBuilder(files);
    }



    public CSVMultiReaderBuilder withSeparator(String separator){
        this.separator = separator;
        return this;
    }

    public CSVMultiReaderBuilder containsHeader(boolean containsHeader){
        this.containsHeader = containsHeader;
        return this;
    }

    public CSVMultiReaderBuilder withHeaderPrefix(String headerPrefix){
        this.headerPrefix = headerPrefix;
        return this;
    }

    public MultiIterator<CSVRow> build(){
        CSVReader[] readers = new CSVReader[files.length];
        for(int i = 0; i < readers.length;i++){
            readers[i] = CSVReaderBuilder.create(files[i])
                    .containsHeader(containsHeader)
                    .withHeaderPrefix(headerPrefix)
                    .withSeparator(separator)
                    .build();
        }
        return MultiIterator.create(readers,CSVRow.class);
    }
}
