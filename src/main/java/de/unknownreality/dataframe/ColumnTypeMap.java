/*
 *
 *  * Copyright (c) 2017 Alexander Grün
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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 22.06.2016.
 */
public class ColumnTypeMap {

    private static final Map<Class<?>, Class<? extends DataFrameColumn>> DEFAULT_COLUMN_TYPES = new HashMap<>();

    static {
        DEFAULT_COLUMN_TYPES.put(String.class, StringColumn.class);
        DEFAULT_COLUMN_TYPES.put(Double.class, DoubleColumn.class);
        DEFAULT_COLUMN_TYPES.put(Integer.class, IntegerColumn.class);
        DEFAULT_COLUMN_TYPES.put(Float.class, FloatColumn.class);
        DEFAULT_COLUMN_TYPES.put(Long.class, LongColumn.class);
        DEFAULT_COLUMN_TYPES.put(Boolean.class, BooleanColumn.class);
        DEFAULT_COLUMN_TYPES.put(Short.class, ShortColumn.class);
        DEFAULT_COLUMN_TYPES.put(Byte.class, ByteColumn.class);
    }


    private final Map<Class<?>, Class<? extends DataFrameColumn>> columnTypesMap = new HashMap<>();

    /** Do not instantiate ColumnConverter. */
    private ColumnTypeMap() {
        this.columnTypesMap.putAll(ColumnTypeMap.DEFAULT_COLUMN_TYPES);
    }

    /**
     * Returns a data frame column for a provided column value type
     *
     * @param type column value class
     * @param <T>  column type
     * @param <C>  value type
     * @return column matching the value type
     */
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> Class<C> getColumnType(Class<T> type) {
        Class<? extends DataFrameColumn> columnType = columnTypesMap.get(type);
        if (columnType == null) {
            return null;
        }
        return (Class<C>) columnType;
    }

    /**
     * Adds a new column type for a column value type
     *
     * @param type       column value class
     * @param columnType column class
     * @param <T>        column value type
     * @param <C>        column type
     * @return <tt>self</tt> for method chaining
     */
    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> ColumnTypeMap addType(Class<T> type, Class<C> columnType) {
        columnTypesMap.put(type, columnType);
        return this;
    }


    /**
     * Creates a column converter instance including the default column types
     *
     * @return column converter
     */
    public static ColumnTypeMap create() {
        return new ColumnTypeMap();
    }


}
