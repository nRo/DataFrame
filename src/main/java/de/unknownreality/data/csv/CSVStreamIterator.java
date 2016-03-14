package de.unknownreality.data.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CSVStreamIterator implements CSVIterator {
    private static Logger log = LoggerFactory.getLogger(CSVStreamIterator.class);

    private BufferedReader reader;
    private CSVRow next;
    private boolean skip;
    private int lineNumber = 0;
    private String separator;
    private int cols = -1;
    private CSVHeader header;
    private CSVRow firstRow;

    public CSVStreamIterator(InputStream stream, CSVHeader header, String separator, boolean skipFirst) {
        this.reader = new BufferedReader(new InputStreamReader(stream));
        this.skip = skipFirst;
        this.separator = separator;
        this.header = header;
        if (skip) {
            skip = false;
            firstRow = getNext();
            next = getNext();
        } else {
            next = getNext();
            firstRow = next;
        }
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            log.error("error closing stream", e);
        }
    }

    public CSVRow getFirstRow() {
        return firstRow;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    private CSVRow getNext() {
        try {
            if (skip) {
                lineNumber++;
                reader.readLine();
                skip = false;
            }
            lineNumber++;
            String line = reader.readLine();
            if (line == null) {
                return null;
            }

            String[] values = line.split(separator);
            if (cols == -1) {
                cols = values.length;
            } else {
                if (values.length != cols) {
                    throw new CSVException(String.format("unequal number of column %d != %d in line %d", values.length, cols, lineNumber));
                }
            }
            for (int i = 0; i < cols; i++) {
                values[i] = values[i].trim();
            }
            return new CSVRow(header, values, lineNumber, separator);

        } catch (IOException e) {
            log.error("error reading file: {}:{}", lineNumber, e);
            close();
        } catch (CSVException e) {
            log.error("error parsing file: {}:{}", lineNumber, e);
            close();
        }
        return null;
    }


    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public CSVRow next() {
        CSVRow row = next;
        next = getNext();
        if (next == null) {
            close();
        }
        return row;
    }
}