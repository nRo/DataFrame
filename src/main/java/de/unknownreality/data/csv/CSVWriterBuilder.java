package de.unknownreality.data.csv;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVWriterBuilder {
    private String separator = ";";
    private String headerPrefix = null;
    private boolean containsHeader = true;


    public static CSVWriterBuilder create() {
        return new CSVWriterBuilder();
    }


    public CSVWriterBuilder withSeparator(String separator) {
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

    public String getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }


    public CSVWriter build() {
        return new CSVWriter(getSeparator(), isContainsHeader(), getHeaderPrefix());
    }


}
