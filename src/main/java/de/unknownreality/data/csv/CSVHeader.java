package de.unknownreality.data.csv;

import de.unknownreality.data.common.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVHeader extends BasicHeader {
    private static Logger log = LoggerFactory.getLogger(CSVHeader.class);



    public void add(){
        String name = "V"+(size()+1);
        super.add(name);
    }

    public static BasicHeader fromLine(String line, String separator){
        CSVHeader header = new CSVHeader();
        header.parse(line,separator,false);
        return header;
    }
    public static BasicHeader fromContentLine(String line, String separator){
        CSVHeader header = new CSVHeader();
        header.parse(line,separator,true);
        return header;
    }


    private void parse(String line, String separator, boolean isContentLine){
        super.clear();
        String[] values = line.split(separator);
        for(int i = 0; i < values.length;i++){
            String name;
            if(!isContentLine){
                name = values[i].trim();
            }
            else{
                name = "V"+(i+1);
            }
            add(name);
        }
    }


}
