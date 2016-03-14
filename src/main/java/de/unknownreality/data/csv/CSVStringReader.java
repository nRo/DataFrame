package de.unknownreality.data.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVStringReader extends  CSVReader{
    private static Logger log = LoggerFactory.getLogger(CSVStringReader.class);
    private String content;
    public CSVStringReader(String content, String separator, boolean containsHeader, String headerPrefix){
        super(separator,containsHeader,headerPrefix);
        this.content = content;
        initHeader();
    }

    public String getContent() {
        return content;
    }

    @Override
    public CSVIterator iterator() {
        return new CSVStreamIterator(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
                ,getHeader(),getSeparator(),containsHeader());
    }
}
