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
    private String encoding = "UTF-8";
    private int skip = 0;
    public CSVStringReader(String content, Character separator, boolean containsHeader,String headerPrefix,String[] ignorePrefixes){
        super(separator,containsHeader,headerPrefix,ignorePrefixes);
        this.content = content;
        initHeader();
        if(containsHeader){
            skip++;
        }

    }

    public String getContent() {
        return content;
    }

    @Override
    public CSVIterator iterator() {

        return new CSVIterator(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
                ,getHeader(),getSeparator(),encoding, getIgnorePrefixes(),skip);
    }
}
