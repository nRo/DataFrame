package de.unknownreality.data.arff;

import de.unknownreality.data.common.RowIterator;
import de.unknownreality.data.common.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public class ARFFIterator implements Iterator<ARFFRow> {
    private static Logger log = LoggerFactory.getLogger(ARFFIterator.class);

    private BufferedReader reader;
    private ARFFRow next;
    private boolean skip;
    private int lineNumber = 0;
    private int cols = -1;
    private ARFFHeader header;

    public ARFFIterator(ARFFHeader header, InputStream stream) {
        this.reader = new BufferedReader(new InputStreamReader(stream));
        this.header = header;
        next = getNext();

    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            log.error("error closing stream", e);
        }
    }



    private ARFFRow getNext() {
        try {
            if (skip) {
                lineNumber++;
                reader.readLine();
                skip = false;
            }
            lineNumber++;
            String line = reader.readLine();
            while(line != null && (line.startsWith("@")||line.startsWith("%") || "".equals(line.trim()))){
                line = reader.readLine();
            }
            if (line == null) {
                return null;
            }
            String[] values = StringUtil.splitQuoted(line,',');
            if (cols == -1) {
                cols = values.length;
            } else {
                if (values.length != cols) {
                    throw new ARFFException(String.format("unequal number of column %d != %d in line %d", values.length, cols, lineNumber));
                }
            }
            for (int i = 0; i < cols; i++) {
                values[i] = values[i].trim();
            }

            return new ARFFRow(header, values, lineNumber);

        } catch (IOException e) {
            log.error("error reading arff file [{}]", lineNumber, e);
            close();
        } catch (ARFFException e) {
            log.error("error parsing arff file [{}]", lineNumber, e);
            close();
        }
        return null;
    }


    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public ARFFRow next() {
        ARFFRow row = next;
        next = getNext();
        if (next == null) {
            close();
        }
        return row;
    }
}