package de.unknownreality.dataframe.common.mapping;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.parser.ParserUtil;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

/**
 * Created by Alex on 08.03.2016.
 */
public class FieldColumn {
    private final Field field;
    private final String headerName;

    public FieldColumn(Field field, String headerName) {
        this.field = field;
        this.headerName = headerName;
    }

    /**
     * Returns the header name of this column
     *
     * @return header name
     */
    public String getHeaderName() {
        return headerName;
    }

    /**
     * Returns the {@link Field} of this field column
     *
     * @return field of the column
     */
    public Field getField() {
        return field;
    }

    /**
     * Converts and inserts a value from a row into an object
     *
     * @param row    row that contains the inserted value
     * @param object object that gets the value inserted
     */
    public void set(Row row,
                    Object object) {
        set(row.get(headerName), object);
    }

    /**
     * Converts the value object and inserts it in the field of an object.
     * The field name is defined as the field name in this object
     *
     * @param value  value that is converted and inserted
     * @param object object that gets the value inserted
     */
    public void set(Object value, Object object) {
        Object convertedVal;
        if (field.getType().isInstance(value)) {
            convertedVal = value;
        } else {
            convertedVal = ParserUtil.parseOrNull(field.getType(), value.toString());
        }
        try {
            if (Modifier.isPublic(field.getModifiers())) {
                field.set(object, convertedVal);
            } else {
                PropertyDescriptor objPropertyDescriptor = new PropertyDescriptor(field.getName(), object.getClass());
                objPropertyDescriptor.getWriteMethod().invoke(object, convertedVal);
            }
        } catch (IllegalAccessException | InvocationTargetException | IntrospectionException e) {
            e.printStackTrace();
        }
    }
}
