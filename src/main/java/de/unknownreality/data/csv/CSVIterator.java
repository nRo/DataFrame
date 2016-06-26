package de.unknownreality.data.csv;

import de.unknownreality.data.common.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;

public class CSVIterator implements Iterator<CSVRow>{
    private static Logger log = LoggerFactory.getLogger(CSVIterator.class);

    private BufferedReader reader;
    private CSVRow next;
    private int lineNumber = 0;
    private Character separator;
    private int cols = -1;
    private CSVHeader header;
    private int skip;
    private String[] ignorePrefixes;
    public CSVIterator(InputStream stream, CSVHeader header, Character separator,String encoding,String[] ignorePrefixes, int skip) {
        this.reader = new BufferedReader(new InputStreamReader(stream));
        this.skip = skip;
        this.separator = separator;
        this.header = header;
        this.ignorePrefixes = ignorePrefixes;
        skip(skip);
        next = getNext();
    }

    public void skip(int rows){
        for(int i = 0; i < rows;i++){
            try {
                reader.readLine();
            } catch (IOException e) {
                log.error("error reading file:{}", e);
                close();
            }
        }
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            log.error("error closing stream", e);
        }
    }



    private CSVRow getNext() {
        try {
            lineNumber++;
            String line = reader.readLine();
            while(line != null && "".equals(line.trim())){
                line = reader.readLine();
            }
            if (line == null) {
                return null;
            }
            for(String prefix : ignorePrefixes){
                if(prefix != null && !"".equals(prefix) && line.startsWith(prefix)){
                    return getNext();
                }
            }
            String[] values = StringUtil.splitQuoted(line,separator);
            if (cols == -1) {
                cols = values.length;
            } else {
                if (values.length != cols) {
                    throw new CSVException(String.format("unequal number of column %d != %d in line %d", values.length, cols, lineNumber));
                }
            }
           // for (int i = 0; i < cols; i++) {
                //values[i] = values[i].trim();
            //}
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


    public boolean hasNext() {
        return next != null;
    }

    public CSVRow next() {
        CSVRow row = next;
        next = getNext();
        if (next == null) {
            close();
        }
        return row;
    }

    @Override
    public void remove() {

    }
}