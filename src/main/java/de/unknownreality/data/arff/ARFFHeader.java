package de.unknownreality.data.arff;

import de.unknownreality.data.common.BasicHeader;
import de.unknownreality.data.common.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 15.03.2016.
 */
public class ARFFHeader extends BasicHeader{
    private static Logger log = LoggerFactory.getLogger(ARFFHeader.class);
    private Map<String,ARFFType> headerTypeMap = new HashMap<>();


    public void add(String name,ARFFType type){
        super.add(name);
        headerTypeMap.put(name,type);
    }

    public ARFFType getType(int index){
        return getType(get(index));
    }

    public ARFFType getType(String name){
        if(!headerTypeMap.containsKey(name)){
            throw new IllegalArgumentException(String.format("column not found '%s'",name));
        }
        return headerTypeMap.get(name);
    }



}
