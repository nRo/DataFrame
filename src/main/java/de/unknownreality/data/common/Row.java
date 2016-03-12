package de.unknownreality.data.common;

/**
 * Created by Alex on 10.03.2016.
 */
public interface Row<T>{
    public T get(int index);
    public T get(String headerName);
    public String getString(int index);
    public String getString(String headerName);
    public Double getDouble(int index);
    public Double getDouble(String headerName);
    public Boolean getBoolean(int index);
    public Boolean getBoolean(String headerName);
    public Integer getInteger(int index);
    public Integer getInteger(String headerName);
    public Float getFloat(int index);
    public Float getFloat(String headerName);
    public<T> T get(String headerName, Class<T> cl);
    public Long getLong(int index);
    public Long getLong(String headerName);}
