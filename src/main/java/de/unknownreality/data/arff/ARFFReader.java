package de.unknownreality.data.arff;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.csv.CSVHeader;
import de.unknownreality.data.csv.CSVRow;
import de.unknownreality.data.frame.DataFrameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Alex on 15.03.2016.
 */
public abstract class ARFFReader implements DataContainer<ARFFHeader,ARFFRow> {
    private static Logger log = LoggerFactory.getLogger(ARFFReader.class);
    private static Pattern ATTRIBUTE_PATTERN = Pattern.compile("@(ATTRIBUTE|attribute)\\s+(.+?)\\s+(.+?)\\s*");

    protected ARFFHeader readHeader(InputStream stream){
        ARFFHeader header = new ARFFHeader();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(stream))){
            String line;
            boolean headerFound = false;
            while((line = reader.readLine()) != null){
                line = line.trim();
                boolean headerLine = line.toUpperCase().startsWith("@ATTRIBUTE");
                if(headerFound && ! headerLine){
                    break;
                }
                if(!headerLine){
                    continue;
                }
                Matcher m = ATTRIBUTE_PATTERN.matcher(line);
                if(!m.matches()){
                    continue;
                }
                String name = m.group(2);
                String type = m.group(3);
                header.add(name,ARFFType.fromString(type));
            }
        } catch (IOException e) {
            log.error("error parsing header",e);
        }
        return header;
    }
    public DataFrameBuilder toDataFrame(){
        return DataFrameBuilder.create(this);
    }
}
