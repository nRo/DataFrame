package de.unknownreality.data.csv;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVWriterBuilder {
    private char separator = ';';
    private String headerPrefix = null;
    private boolean containsHeader = true;
    private boolean gzip = false;

    public static CSVWriterBuilder create() {
        return new CSVWriterBuilder();
    }


    public CSVWriterBuilder withSeparator(char separator) {
        this.separator = separator;
        return this;
    }

    public CSVWriterBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }


    public CSVWriterBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    public CSVWriterBuilder useGzip(boolean gzip) {
        this.gzip = gzip;
        return this;
    }


    public char getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }

    public boolean isGzip() {
        return gzip;
    }

    public CSVWriter build() {
        return new CSVWriter(getSeparator(), isContainsHeader(), getHeaderPrefix(),isGzip());
    }


}
