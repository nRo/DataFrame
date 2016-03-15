package de.unknownreality.data.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Iterator;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVFileReader extends  CSVReader{
    private static Logger log = LoggerFactory.getLogger(CSVFileReader.class);
    private File file;
    public CSVFileReader(File file,Character separator, boolean containsHeader,String headerPrefix){
        super(separator,containsHeader,headerPrefix);
        this.file = file;
        initHeader();
    }
    @Override
    public CSVIterator iterator() {
        try{
            return new CSVIterator(new FileInputStream(file),getHeader(),getSeparator(),containsHeader());
        } catch (FileNotFoundException e) {
            log.error("file not found: {}",file,e);
        }
        return null;
    }
}
