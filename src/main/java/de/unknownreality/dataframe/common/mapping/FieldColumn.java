/*
 *
 *  * Copyright (c) 2019 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.common.mapping;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.type.DataFrameTypeManager;

import java.lang.reflect.Field;

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
    public void set(Row<?, String> row,
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
            convertedVal = DataFrameTypeManager.get().parseOrNull(field.getType(), value.toString());
        }
        try {
            field.setAccessible(true);
            field.set(object, convertedVal);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
