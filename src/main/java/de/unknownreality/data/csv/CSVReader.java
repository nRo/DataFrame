package de.unknownreality.data.csv;

import de.unknownreality.data.common.RowIterator;
import de.unknownreality.data.csv.mapping.CSVMapper;
import de.unknownreality.data.frame.DataFrameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVReader implements RowIterator<CSVRow>{
    private static Logger log = LoggerFactory.getLogger(CSVReader.class);
    private String headerPrefix = "#";
    private String separator;
    private File file;
    private CSVHeader header;
    private boolean containsHeader;

    protected CSVReader(File file, String separator, boolean containsHeader,String headerPrefix){
        this.file = file;
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
        initHeader();
    }

    public CSVHeader getHeader() {
        return header;
    }

    private void initHeader(){
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            String headerLine = reader.readLine();
            if(headerLine == null){
                containsHeader = false;
                header = new CSVHeader();
                return;
            }
            if(containsHeader){
                if(!headerLine.startsWith(headerPrefix)){
                    throw new CSVException(file,"invalid header prefix in first line");
                }
                headerLine = headerPrefix == null ? headerLine : headerLine.substring(headerPrefix.length());
                header = CSVHeader.fromLine(headerLine, separator);
            }
            else{
                header = CSVHeader.fromContentLine(headerLine, separator);
                containsHeader = false;
            }
        } catch (FileNotFoundException e) {
            log.error("file not found: {}",file,e);
        } catch (IOException e) {
            log.error("error reading file: {}",file,e);
        } catch (CSVException e) {
            log.error("error reading file: {}",file,e);
        }
    }

    @Override
    public Iterator<CSVRow> iterator() {
        return new CSVIterator(file,header, separator,containsHeader);
    }

    public DataFrameBuilder buildDataFrame(){
        return DataFrameBuilder.create(this);
    }
    public <T> List<T> map(Class<T> cl){
        return CSVMapper.map(this,cl);
    }



    private static class CSVIterator implements Iterator<CSVRow>{
        private static Logger log = LoggerFactory.getLogger(CSVIterator.class);

        private File file;
        private BufferedReader reader;
        private CSVRow next;
        private boolean skip;
        private int lineNumber = 0;
        private String separator;
        private int cols = -1;
        private CSVHeader header;
        public CSVIterator(File file,CSVHeader header,String separator, boolean skipFirst){
            this.file = file;
            this.skip = skipFirst;
            this.separator = separator;
            this.header = header;
            open();
            next = getNext();
        }

        private void open()  {
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                log.error("file not found: {}",file,e);
            }
        }

        private void close() {
            try {
                reader.close();
            } catch (IOException e) {
                log.error("error closing file: {}",file,e);
            }
        }

        private CSVRow getNext(){
            try {
                if(skip){
                    lineNumber++;
                    reader.readLine();
                    skip = false;
                }
                lineNumber++;
                String line = reader.readLine();
                if(line == null){
                    return null;
                }

                String[] values = line.split(separator);
                if(cols == -1){
                    cols = values.length;
                }
                else{
                    if(values.length != cols){
                        throw new CSVException(file,String.format("unequal number of column %d != %d in line %d",values.length,cols,lineNumber));
                    }
                }
                for(int i = 0; i < cols;i++){
                    values[i] = values[i].trim();
                }
                return new CSVRow(header,values,lineNumber,separator);

            } catch (IOException e) {
                log.error("error reading file: {}:{}",file,lineNumber,e);
                close();
            } catch (CSVException e) {
                log.error("error parsing file: {}:{}",file,lineNumber,e);
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
            if(next == null){
                close();
            }
            return row;
        }
    }
}
