package de.unknownreality.data.common;

import de.unknownreality.data.frame.DataFrame;

import java.io.File;
import java.util.Map;

/**
 * Created by Alex on 14.03.2016.
 */
public interface DataWriter {
    public void write(File file, DataContainer<? extends Header,? extends Row> dataContainer);
    public void write(File file, DataFrame dataFrame, boolean writeMetaFile);
    public void print(DataContainer<? extends Header,? extends Row> dataContainer);
    public Map<String,String> getAttributes();
}
