package de.unknownreality.data.csv;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Created by Alex on 12.03.2016.
 */
public interface CSVIterator extends Iterator<CSVRow>, Closeable{
    public void setSkip(boolean skip);
    public CSVRow getFirstRow();
}
