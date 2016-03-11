package de.unknownreality.data.csv.mapping;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.Parsers;
import de.unknownreality.data.csv.CSVRow;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

/**
 * Created by Alex on 08.03.2016.
 */
public class FieldColumn {
    private Field field;
    private String headerName;

    public FieldColumn(Field field, String headerName){
        this.field = field;
        this.headerName = headerName;
    }

    public String getHeaderName() {
        return headerName;
    }

    public Field getField() {
        return field;
    }

    public void set(CSVRow row,
                    Object object){
        set(row.get(headerName),object);
    }
    public void set(String value, Object object){
        Object convertedVal = Parsers.parseOrNull(field.getType(),value);
        try {
            if(Modifier.isPublic(field.getModifiers())){
                field.set(object,convertedVal);
            }
           else{
                PropertyDescriptor objPropertyDescriptor = new PropertyDescriptor(field.getName(), object.getClass());
                objPropertyDescriptor.getWriteMethod().invoke(object,convertedVal);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IntrospectionException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
