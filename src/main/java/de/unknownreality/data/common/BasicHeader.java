package de.unknownreality.data.common;

import de.unknownreality.data.common.Header;

import java.util.*;

/**
 * Created by Alex on 15.03.2016.
 */
public class BasicHeader implements Header<String> {
    private Map<String,Integer> headerMap = new HashMap<>();
    private List<String> headers = new ArrayList<>();

    public int size(){
        return headers.size();
    }

    public void add(String name){
        headerMap.put(name,headers.size());
        headers.add(name);
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

    public void clear(){
        headerMap.clear();
        headers.clear();
    }

    public Iterator<String> iterator() {
        return headers.iterator();
    }
}
