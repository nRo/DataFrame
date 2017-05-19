package de.unknownreality.dataframe.common.reader;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.StringUtil;
import de.unknownreality.dataframe.csv.CSVException;
import de.unknownreality.dataframe.csv.CSVIterator;
import de.unknownreality.dataframe.csv.CSVRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Created by Alex on 19.05.2017.
 */
public abstract class BufferedStreamIterator<R extends Row> implements Iterator<R> {
    private static final Logger log = LoggerFactory.getLogger(BufferedStreamIterator.class);
    private final BufferedReader reader;
    private R next;


    public BufferedStreamIterator(InputStream stream) {
        if (stream == null) {
            throw new RuntimeException("input stream is null");
        }
        this.reader = new BufferedReader(new InputStreamReader(stream));
    }

    protected void loadNext(){
        next = getNext();
    }

    protected String getLine() throws IOException {
        return reader.readLine();
    }

    /**
     * skips rows
     *
     * @param rows number of rows to be skipped
     */
    public void skip(int rows) {
        for (int i = 0; i < rows; i++) {
            try {
                getLine();
            } catch (IOException e) {
                log.error("error reading file:{}", e);
                close();
            }
        }
    }

    /**
     * closes this iterator
     */
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            log.error("error closing stream", e);
        }
    }


    /**
     * Reads the csv input stream and returns a row
     *
     * @return next row
     */
    protected abstract R getNext();

    /**
     * Returns true if last row is not reached yet
     *
     * @return true if next row exists
     */
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next row.
     * If last row is reached this iterator closes automatically.
     *
     * @return next row
     */
    public R next() {
        R row = next;
        next = getNext();
        if (next == null) {
            close();
        }
        return row;
    }

    /**
     * Remove is not supported
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove not supported by this iterator");
    }

}
