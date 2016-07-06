package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.GZipUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVFileReader extends  CSVReader{
    private static Logger log = LoggerFactory.getLogger(CSVFileReader.class);
    private File file;
    private String encoding = "UTF-8";
    private boolean gzipped;
    private int skip = 0;
    public CSVFileReader(File file,Character separator, boolean containsHeader,String headerPrefix, String[] ignorePrefixes){
        super(separator,containsHeader,headerPrefix,ignorePrefixes);
        this.file = file;
        gzipped = GZipUtil.isGzipped(file);
        initHeader();
        if(containsHeader){
            skip++;
        }
    }
    @Override
    public CSVIterator iterator() {
        try{
            InputStream inputStream = new FileInputStream(file);
            if(gzipped){
                try {
                    inputStream = new GZIPInputStream(inputStream);
                } catch (IOException e) {
                    log.error("error creating gzip input stream",e);
                }
            }
            return new CSVIterator(inputStream,getHeader(),getSeparator(),encoding, getIgnorePrefixes(),skip);
        } catch (FileNotFoundException e) {
            log.error("file not found: {}",file,e);
        }
        return null;
    }
}
