package de.unknownreality.data.csv;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVReaderBuilder {
    private String separator = "\t";
    private String headerPrefix = "#";
    private boolean containsHeader = true;


    public static CSVReaderBuilder create() {
        return new CSVReaderBuilder();
    }


    public CSVReaderBuilder withSeparator(String separator) {
        this.separator = separator;
        return this;
    }

    public CSVReaderBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }

    public CSVReaderBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    public String getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }


    public CSVReader load(File file) {
        return new CSVFileReader(file, getSeparator(), isContainsHeader(), getHeaderPrefix());
    }

    public CSVReader load(String content) {
        return new CSVStringReader(content, getSeparator(), isContainsHeader(), getHeaderPrefix());
    }

}
