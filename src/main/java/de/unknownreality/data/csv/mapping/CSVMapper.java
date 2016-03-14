package de.unknownreality.data.csv.mapping;

import de.unknownreality.data.csv.CSVHeader;
import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.csv.CSVRow;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 08.03.2016.
 */
public class CSVMapper<T> {
    private CSVReader reader;
    private FieldColumn[] columns;
    private Map<Integer,FieldColumn> columnMap = new HashMap<>();
    private Class<T> cl;
    private CSVMapper(CSVReader reader, Class<T> cl){
            this.reader = reader;
            this.cl = cl;
    }

    public static <T> List<T> map(CSVReader reader,Class<T> cl){
        CSVMapper<T> mapper = new CSVMapper<>(reader,cl);
        return mapper.map();
    }


    public List<T> map(){
        List<T> result = new ArrayList<>();
        initFields(reader.getHeader());
        for(CSVRow row : reader){
            result.add(processRow(row));
        }
        return result;
    }
    private void initFields(CSVHeader header) {

        List<FieldColumn> fieldColumnList = new ArrayList<>();
        for (Field field : cl.getDeclaredFields()) {
            String name = field.getName();
            CSVColumn annotation = field.getAnnotation(CSVColumn.class);
            if (annotation == null) {
                continue;
            }
            String headerName = annotation.header();
            if (!isValid(headerName, header)) {
                if (annotation.index() != -1) {
                    if (annotation.index() < header.size()) {
                        headerName = header.get(annotation.index());
                    }
                }
            }
            if (!isValid(headerName, header)) {
                if (isValid(name, header)) {
                    headerName = name;
                } else {
                    System.err.println(annotation.toString() + " not found in file");
                    continue;
                }
            }

            fieldColumnList.add(new FieldColumn(field, headerName));
        }
        columns = new FieldColumn[fieldColumnList.size()];
        fieldColumnList.toArray(columns);
    }

    private boolean isValid(String headerName, CSVHeader header){
        return !"".equals(headerName) && header.contains(headerName);
    }


    private T processRow(CSVRow row){
        T obj = null;
        try {
            obj = cl.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        for(FieldColumn fieldColumn : columns){
            fieldColumn.set(row,obj);
        }
        return obj;
    }
}
