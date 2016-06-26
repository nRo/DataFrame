package de.unknownreality.data.csv;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.ReaderBuilder;
import de.unknownreality.data.common.parser.ParserUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVReaderBuilder implements ReaderBuilder<CSVHeader,CSVRow>{
    private Character separator = '\t';
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private List<String> ignorePrefixes = new ArrayList<>();

    public static CSVReaderBuilder create() {
        return new CSVReaderBuilder();
    }

    public CSVReaderBuilder(){

    }

    public CSVReaderBuilder withSeparator(Character separator) {
        this.separator = separator;
        return this;
    }

    public CSVReaderBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }

    public CSVReaderBuilder addIgnorePrefix(String prefix){
            ignorePrefixes.add(prefix);
            return this;
    }



    public CSVReaderBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    public Character getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }


    public CSVReader load(File file) {
        String[] ignorePrefixesArray = new String[this.ignorePrefixes.size()];
        this.ignorePrefixes.toArray(ignorePrefixesArray);
        return new CSVFileReader(file, getSeparator(), isContainsHeader(),getHeaderPrefix(),ignorePrefixesArray);
    }

    public CSVReader load(String content) {
        String[] ignorePrefixesArray = new String[this.ignorePrefixes.size()];
        this.ignorePrefixes.toArray(ignorePrefixesArray);
        return new CSVStringReader(content, getSeparator(), isContainsHeader(),getHeaderPrefix(),ignorePrefixesArray);
    }

    @Override
    public void loadAttributes(Map<String, String> attributes)  throws Exception{
        this.separator = ParserUtil.parse(Character.class, attributes.get("separator"));
        this.headerPrefix = attributes.get("headerPrefix");
        this.containsHeader = ParserUtil.parse(Boolean.class, attributes.get("containsHeader"));
    }

    @Override
    public DataContainer<CSVHeader, CSVRow> fromFile(File f) {
        return load(f);
    }

    @Override
    public DataContainer<CSVHeader, CSVRow> fromString(String content) {
        return load(content);
    }
}
