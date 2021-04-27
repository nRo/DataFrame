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

package de.unknownreality.dataframe.common.row;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.header.Header;
import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.DataFrameTypeManager;
import de.unknownreality.dataframe.type.ValueType;
import de.unknownreality.dataframe.type.ValueTypeNotFoundException;
import de.unknownreality.dataframe.type.impl.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Alex on 19.05.2017.
 */
public class StringRow<T, H extends Header<T>> implements Row<String, T>, Iterable<String> {
    private static final Logger log = LoggerFactory.getLogger(StringRow.class);

    private final static StringType STRING_VALUE_TYPE = new StringType(new ColumnSettings());

    private static final ValueType<Boolean> BOOLEAN_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Boolean.class);
    private static final ValueType<Double> DOUBLE_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Double.class);
    private static final ValueType<Float> FLOAT_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Float.class);
    private static final ValueType<Long> LONG_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Long.class);
    private static final ValueType<Integer> INTEGER_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Integer.class);
    private static final ValueType<Short> SHORT_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Short.class);
    private static final ValueType<Byte> BYTE_VALUE_READER = DataFrameTypeManager.get().findValueTypeOrThrow(Byte.class);


    private final String[] values;
    private final H header;
    private final int rowNumber;

    public StringRow(H header, String[] values, int rowNumber) {
        this.values = values;
        this.header = header;
        this.rowNumber = rowNumber;
    }

    /**
     * Returns the number of this row
     *
     * @return row number
     */
    public int getRowNumber() {
        return rowNumber;
    }

    /**
     * Returns the values of this row as string array
     *
     * @return values array
     */
    public String[] getValues() {
        return values;
    }

    @Override
    public String get(int index) {
        if (index >= values.length) {
            throw new IllegalArgumentException(String.format("header index out of bounds %d > %d", index, (values.length - 1)));
        }
        return values[index];
    }

    @Override
    public String get(T headerName) {
        int index = header.getIndex(headerName);
        return values[index];
    }

    @Override
    public String getString(int index) {
        return get(index);
    }

    @Override
    public String getString(T headerName) {
        return get(headerName);
    }


    @Override
    public Boolean getBoolean(int index) {
        return parse(index, Boolean.class, BOOLEAN_VALUE_READER);

    }

    @Override
    public Boolean getBoolean(T header) {
        return parse(header, Boolean.class, BOOLEAN_VALUE_READER);

    }

    @Override
    public Double getDouble(int index) {
        return parse(index, Double.class, DOUBLE_VALUE_READER);

    }

    @Override
    public Double getDouble(T header) {
        return parse(header, Double.class, DOUBLE_VALUE_READER);
    }


    @Override
    public Long getLong(int index) {
        return parse(index, Long.class, LONG_VALUE_READER);
    }

    @Override
    public Long getLong(T header) {
        return parse(header, Long.class, LONG_VALUE_READER);
    }

    @Override
    public Short getShort(int index) {
        return parse(index, Short.class, SHORT_VALUE_READER);
    }

    @Override
    public Short getShort(T headerName) {
        return parse(headerName, Short.class, SHORT_VALUE_READER);
    }

    @Override
    public Byte getByte(int index) {
        return parse(index, Byte.class, BYTE_VALUE_READER);
    }

    @Override
    public Byte getByte(T headerName) {
        return parse(headerName, Byte.class, BYTE_VALUE_READER);
    }

    @Override
    public Integer getInteger(int index) {
        return parse(index, Integer.class, INTEGER_VALUE_READER);
    }

    @Override
    public Integer getInteger(T header) {
        return parse(header, Integer.class, INTEGER_VALUE_READER);
    }

    @Override
    public Float getFloat(int index) {
        return parse(index, Float.class, FLOAT_VALUE_READER);
    }

    @Override
    public Float getFloat(T header) {
        return parse(header, Float.class, FLOAT_VALUE_READER);
    }


    /**
     * Gets a value by its column header name and parses it into a specified type
     * This method throws a {@link DataFrameRuntimeException} if anything goes wrong.
     *
     * @param name        csv column name
     * @param cl          class of resulting entity
     * @param valueReader used parser
     * @param <C>         type of resulting entity
     * @return parsed entity
     */
    protected <C> C parse(T name, Class<C> cl, ValueType<C> valueReader) {
        String val = get(name);
        try {
            return valueReader.parse(val);
        } catch (ParseException e) {
            log.error("error parsing value {} to {}", val, cl, e);
            throw new DataFrameRuntimeException(String.format("error parsing value %s to %s", val, cl), e);
        }
    }

    /**
     * Gets a value by its index and parses it into a specified type
     * This method throws a {@link DataFrameRuntimeException} if anything goes wrong.
     *
     * @param index       csv column index
     * @param cl          class of resulting entity
     * @param valueReader used parser
     * @param <C>         type of resulting entity
     * @return parsed entity
     */
    protected <C> C parse(int index, Class<C> cl, ValueType<C> valueReader) {
        String val = get(index);
        try {
            return valueReader.parse(val);
        } catch (ParseException e) {
            log.error("error parsing value {} to {}", val, cl, e);
            throw new DataFrameRuntimeException(String.format("error parsing value %s to %s", val, cl), e);
        }
    }

    @Override
    public ValueType<String> getType(int index) {
        return STRING_VALUE_TYPE;
    }

    @Override
    public ValueType<String> getType(T headerName) {
        return STRING_VALUE_TYPE;
    }

    @Override
    public <C> C get(T headerName, Class<C> cl) {
        return getValueAs(get(headerName), cl);
    }

    @Override
    public <C> C getOrNull(T headerName, Class<C> cl) {
        return getValueAsOrNull(get(headerName), cl);
    }

    @Override
    public <C> C get(int index, Class<C> cl) {
        return getValueAs(get(index), cl);
    }

    @Override
    public <C> C getOrNull(int index, Class<C> cl) {
        return getValueAsOrNull(get(index), cl);
    }

    /**
     * Converts a value to a specific type.
     * This method throws a {@link DataFrameRuntimeException} if anything goes wrong.
     *
     * @param value value to convert
     * @param cl    resulting class
     * @param <C>   resulting type
     * @return converted value
     */
    protected <C> C getValueAs(String value, Class<C> cl) {
        try {
            return DataFrameTypeManager.get().parse(cl, value);
        } catch (ParseException | ValueTypeNotFoundException e) {
            log.error("error parsing value {} to {}", value, cl, e);
            throw new DataFrameRuntimeException(String.format("error parsing value %s to %s", value, cl), e);

        }
    }

    /**
     * Converts a value to a specific type.
     * This method returns <tt>null</tt> if anything goes wrong.
     *
     * @param value value to convert
     * @param cl    resulting class
     * @param <C>   resulting type
     * @return converted value
     */
    protected <C> C getValueAsOrNull(String value, Class<C> cl) {
        try {
            return DataFrameTypeManager.get().parse(cl, value);
        } catch (ParseException | ValueTypeNotFoundException e) {
            log.warn("error parsing value {} to {}", value, cl, e);

        }
        return null;
    }

    @Override
    public int size() {
        return values.length;
    }

    /**
     * Returns an iterator over the entities in this csv row.
     * Each entity is represented as {@link String}
     *
     * @return row entity iterator
     */
    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < values.length - 1;
            }

            @Override
            public String next() {
                if (index >= values.length) {
                    throw new NoSuchElementException(String.format("element not found: index out of bounds %s >= %s]", index, values.length));
                }
                return values[index++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported by CSVRows");
            }

        };
    }

    /**
     * Returns the row as string.
     * The csv separator char is used to join the values
     *
     * @return row string
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String item : values) {
            if (first)
                first = false;
            else
                sb.append('\t');
            sb.append(item);
        }
        return sb.toString();
    }
}
