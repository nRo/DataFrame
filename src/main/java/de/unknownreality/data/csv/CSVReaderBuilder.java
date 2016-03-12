package de.unknownreality.data.csv;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class CSVReaderBuilder {
    private String separator = "\t";
    private String headerPrefix = "#";
    private boolean containsHeader = true;


    public static CSVFileReaderBuilder create(File file) {
        return new CSVFileReaderBuilder(file);
    }

    public static CSVStringReaderBuilder create(String string) {
        return new CSVStringReaderBuilder(string);
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

    public abstract CSVReader build();


    public static class CSVFileReaderBuilder extends CSVReaderBuilder {
        private File file;
        private CSVFileReaderBuilder(File file){
            this.file = file;
        }
        public CSVReader build() {
            return new CSVFileReader(file, getSeparator(), isContainsHeader(), getHeaderPrefix());
        }

    }
    public static class CSVStringReaderBuilder extends CSVReaderBuilder {
        private String content;
        private CSVStringReaderBuilder(String content){
            this.content = content;
        }
        public CSVReader build() {
            return new CSVStringReader(content, getSeparator(), isContainsHeader(), getHeaderPrefix());
        }

    }
}
