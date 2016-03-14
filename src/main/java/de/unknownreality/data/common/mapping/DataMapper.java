package de.unknownreality.data.common.mapping;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.Header;
import de.unknownreality.data.common.Row;
import de.unknownreality.data.csv.CSVReader;
import de.unknownreality.data.csv.CSVRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 08.03.2016.
 */
public class DataMapper<T> {
    private static Logger log = LoggerFactory.getLogger(DataMapper.class);
    private DataContainer<? extends Header,? extends Row> reader;
    private FieldColumn[] columns;
    private Map<Integer,FieldColumn> columnMap = new HashMap<>();
    private Class<T> cl;
    private DataMapper(DataContainer<? extends Header,? extends Row> reader, Class<T> cl){
            this.reader = reader;
            this.cl = cl;
    }

    public static <T> List<T> map(DataContainer<? extends Header,? extends Row> reader,Class<T> cl){
        DataMapper<T> mapper = new DataMapper<>(reader,cl);
        return mapper.map();
    }


    public List<T> map(){
        List<T> result = new ArrayList<>();
        initFields(reader.getHeader());
        for(Row row : reader){
            result.add(processRow(row));
        }
        return result;
    }
    private void initFields(Header header) {

        List<FieldColumn> fieldColumnList = new ArrayList<>();
        for (Field field : cl.getDeclaredFields()) {
            String name = field.getName();
            MappedColumn annotation = field.getAnnotation(MappedColumn.class);
            if (annotation == null) {
                continue;
            }
            String headerName = annotation.header();
            if (!isValid(headerName, header)) {
                if (annotation.index() != -1) {
                    if (annotation.index() < header.size()) {
                        headerName = header.get(annotation.index()).toString();
                    }
                }
            }
            if (!isValid(headerName, header)) {
                if (isValid(name, header)) {
                    headerName = name;
                } else {
                    log.error("{} not found in file",annotation.toString());
                    continue;
                }
            }

            fieldColumnList.add(new FieldColumn(field, headerName));
        }
        columns = new FieldColumn[fieldColumnList.size()];
        fieldColumnList.toArray(columns);
    }

    private boolean isValid(Object headerName, Header header){
        return !"".equals(headerName) && header.contains(headerName);
    }


    private T processRow(Row row){
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
