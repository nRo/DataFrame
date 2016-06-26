package de.unknownreality.data.common;

import java.io.File;
import java.util.Map;

/**
 * Created by Alex on 07.06.2016.
 */
public interface ReaderBuilder<H extends Header,R extends Row>{
    public void loadAttributes(Map<String,String> attributes) throws Exception;
    public DataContainer<H,R> fromFile(File f);
    public DataContainer<H,R> fromString(String content);
}
