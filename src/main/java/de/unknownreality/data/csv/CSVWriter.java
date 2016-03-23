package de.unknownreality.data.csv;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.DataWriter;
import de.unknownreality.data.common.Header;
import de.unknownreality.data.common.Row;
import de.unknownreality.data.frame.DataFrame;
import de.unknownreality.data.frame.DataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Alex on 14.03.2016.
 */
public class CSVWriter implements DataWriter{
    private static Logger log = LoggerFactory.getLogger(CSVWriter.class);

    private String separator = "\t";
    private String headerPrefix = "#";
    private boolean containsHeader = true;

    protected CSVWriter(String separator, boolean containsHeader,String headerPrefix){
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
    }
    @Override
    public void write(File file, DataContainer<? extends Header,? extends Row> dataContainer) {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))){
            if(containsHeader){
                if(headerPrefix != null){
                    writer.write(headerPrefix);
                }
                for(int i = 0; i < dataContainer.getHeader().size();i++){
                    writer.write(dataContainer.getHeader().get(i).toString());
                    if(i < dataContainer.getHeader().size() - 1){
                        writer.write(separator);
                    }
                }
                writer.newLine();
            }
            for(Row row : dataContainer){
                for(int i = 0; i < row.size();i++){
                    writer.write(row.get(i).toString());
                    if(i < row.size() - 1){
                        writer.write(separator);
                    }
                }
                writer.newLine();
            }
        } catch (IOException e) {
            log.error("error writing {}",file,e);
        }
    }

    @Override
    public void print(DataContainer<? extends Header, ? extends Row> dataContainer) {
        if(containsHeader){
            if(headerPrefix != null){
                System.out.print(headerPrefix);
            }
            for(int i = 0; i < dataContainer.getHeader().size();i++){
                System.out.print(dataContainer.getHeader().get(i).toString());
                if(i < dataContainer.getHeader().size() - 1){
                    System.out.print(separator);
                }
            }
            System.out.println();
        }
        for(Row row : dataContainer){
            for(int i = 0; i < row.size();i++){
                System.out.print(row.get(i).toString());
                if(i < row.size() - 1){
                    System.out.print(separator);
                }
            }
            System.out.println();
        }
    }
}
