package de.unknownreality.data.common;

import java.io.File;

/**
 * Created by Alex on 14.03.2016.
 */
public interface DataWriter {
    public void write(File file, DataContainer<? extends Header,? extends Row> dataContainer);
    public void print(DataContainer<? extends Header,? extends Row> dataContainer);
}
