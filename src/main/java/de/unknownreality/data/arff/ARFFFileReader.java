package de.unknownreality.data.arff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by Alex on 12.03.2016.
 */
public class ARFFFileReader extends  ARFFReader{
    private static Logger log = LoggerFactory.getLogger(ARFFFileReader.class);
    private File file;
    private ARFFHeader header;
    public ARFFFileReader(File file){
        this.file = file;
        initHeader();
    }
    private void initHeader(){
        try{
            this.header = readHeader(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            log.error("file not found: {}",file,e);
        }
    }

    @Override
    public ARFFHeader getHeader() {
        return header;
    }

    @Override
    public ARFFIterator iterator() {
        try{
            return new ARFFIterator(getHeader(),new FileInputStream(file));
        } catch (FileNotFoundException e) {
            log.error("file not found: {}",file,e);
        }
        return null;
    }
}
