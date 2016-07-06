package de.unknownreality.dataframe.frame.group;

import de.unknownreality.dataframe.common.Header;

import java.util.*;

/**
 * Created by Alex on 11.03.2016.
 */
public class GroupHeader implements Header<String> {
    private Map<String,Integer> headerMap = new HashMap<>();
    private List<String> headers = new ArrayList<>();
    public GroupHeader(String... columns){
        for(int i = 0; i< columns.length;i++){
            headers.add(columns[i]);
            headerMap.put(columns[i],i);
        }
    }
    @Override
    public int size() {
        return headers.size();
    }

    @Override
    public String get(int index) {
        if(index >= headers.size()){
            throw new IllegalArgumentException(String.format("header index out of bounds %d > %d",index,(headers.size()-1)));
        }
        return headers.get(index);
    }

    @Override
    public boolean contains(String value) {
        return headerMap.containsKey(value);
    }

    @Override
    public int getIndex(String name) {
        return headerMap.get(name);
    }


    @Override
    public Iterator<String> iterator() {
        return headers.iterator();
    }
}
