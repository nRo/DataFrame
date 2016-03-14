package de.unknownreality.data.common.mapping;

import de.unknownreality.data.common.Row;
import de.unknownreality.data.common.parser.ParserUtil;
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

    public void set(Row row,
                    Object object){
        set(row.get(headerName),object);
    }
    public void set(Object value, Object object){
        Object convertedVal;
        if(field.getType().isInstance(value)){
            convertedVal = value;
        }
        else{
            convertedVal = ParserUtil.parseOrNull(field.getType(),value.toString());
        }
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
