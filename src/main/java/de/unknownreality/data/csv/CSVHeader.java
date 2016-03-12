package de.unknownreality.data.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVHeader  implements Iterable<String>{
    private static Logger log = LoggerFactory.getLogger(CSVHeader.class);
    private Map<String,Integer> headerMap = new HashMap<>();
    private List<String> headers = new ArrayList<>();

    public int size(){
        return headers.size();
    }

    public String get(int index){
        if(index >= headers.size()){
            throw new IllegalArgumentException(String.format("header index out of bounds %d > %d",index,(headers.size()-1)));
        }
        return headers.get(index);
    }

    public boolean contains(String name){
        return headerMap.containsKey(name);
    }

    public int getIndex(String name){
        Integer index = headerMap.get(name);
        index = index == null ? -1 : index;
        return index;
    }

    public static CSVHeader fromLine(String line,String separator){
        CSVHeader header = new CSVHeader();
        header.parse(line,separator,false);
        return header;
    }
    public static CSVHeader fromContentLine(String line,String separator){
        CSVHeader header = new CSVHeader();
        header.parse(line,separator,true);
        return header;
    }

    private void parse(String line, String separator, boolean isContentLine){
        headerMap.clear();
        headers.clear();
        String[] values = line.split(separator);
        for(int i = 0; i < values.length;i++){
            String name;
            if(!isContentLine){
                name = values[i].trim();
            }
            else{
                name = "V"+(i+1);
            }
            headerMap.put(name,i);
            headers.add(name);
        }
    }


    @Override
    public Iterator<String> iterator() {
        return headers.iterator();
    }

}
