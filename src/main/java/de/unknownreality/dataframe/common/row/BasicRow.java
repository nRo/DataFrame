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

package de.unknownreality.dataframe.common.row;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.common.Header;
import de.unknownreality.dataframe.common.Row;

/**
 * Created by Alex on 19.05.2017.
 */
public class BasicRow<T,H extends Header<T>,V> implements Row<V,T> {
    private final V[] values;
    private final H header;
    private final int index;

    public BasicRow(H header, V[] values, int index) {
        this.header = header;
        this.values = values;
        this.index = index;
    }

    /**
     * Get index of data row
     *
     * @return data row index
     */
    public int getIndex() {
        return index;
    }

    public V get(T headerName) {
        int headerIndex = header.getIndex(headerName);
        return this.values[headerIndex];
    }

    protected H getHeader(){
        return header;
    }

    @Override
    public V get(int index) {
        return this.values[index];
    }

    @Override
    public Double getDouble(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(get(index)).doubleValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no double value in col " + index + " (" + value + ")");
        }
    }

    /**
     * Returns the values of this row as array
     *
     * @return values array
     */
    protected V[] getValues() {
        return values;
    }

    public String getString(int index) {
        Object value = get(index);
        if (value != null) {
            return value.toString();
        }
        throw new DataFrameRuntimeException("no String value in col " + index + " (null)");
    }

    @Override
    public int size() {
        return values.length;
    }


    @Override
    public Boolean getBoolean(int index) {
        Object value = get(index);
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        throw new DataFrameRuntimeException("no boolean value in col " + index + " (" + value + ")");
    }

    @Override
    public Double getDouble(T name) {
        Object value = get(name);
        try {
            return Number.class.cast(get(name)).doubleValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no double value in col " + name + " (" + value + ")");
        }
    }


    /**
     * Returns entity from an column index as {@link Number}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index column index
     * @return {@link Short} entity
     */
    public Number getNumber(int index) {
        Object value = get(index);
        if (value instanceof Number) {
            return (Number) value;
        }
        throw new DataFrameRuntimeException("no number value in col " + index + " (" + value + ")");
    }


    /**
     * Returns entity from header name column as {@link Number}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param name header name
     * @return {@link Number} entity
     */
    public Number getNumber(T name) {
        return getNumber(header.getIndex(name));
    }


    @Override
    public String getString(T name) {
        return getString(header.getIndex(name));
    }

    @Override
    public Boolean getBoolean(T name) {
        return getBoolean(header.getIndex(name));

    }

    /**
     * Returns the double value at a specified index.
     * If no double value is found, the value is parsed to double
     * If the value could not be parsed Double.NaN is returned
     * @param index index of value
     * @return double value
     */
    public Double toDouble(int index){
        V v = get(index);
        try{
            return Number.class.cast(v).doubleValue();
        }
        catch (Exception e){
            // try parsing now
        }
        try{
            return Double.parseDouble(String.valueOf(v));
        }
        catch (Exception e){
            return Double.NaN;
        }
    }

    /**
     * Returns the double value at a specified header position
     * @see #toDouble(int)
     * @param name header name
     * @return double value
     */
    public Double toDouble(T name){
        return toDouble(header.getIndex(name));
    }

    @Override
    public Integer getInteger(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(get(index)).intValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no int value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Integer getInteger(T headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).intValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no int value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no float value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(T headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no float value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Long getLong(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).longValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no long value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Long getLong(T headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).longValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no long value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Short getShort(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).shortValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no short value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Short getShort(T headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).shortValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no short value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Byte getByte(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).byteValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no byte value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Byte getByte(T headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).byteValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no byte value in col " + index + " (" + value + ")");
        }
    }

    /**
     * Returns <tt>true</tt> if the value at the specified column name is {@link Values#NA NA}.
     *
     * @param headerName column name
     * @return <tt>true</tt> if the value in this column is NA
     */
    public boolean isNA(T headerName) {
        return get(headerName) == Values.NA || get(headerName) == null;
    }


    /**
     * Returns <tt>true</tt> if the value at the specified index is {@link Values#NA NA}.
     *
     * @param index column index
     * @return <tt>true</tt> if the value in this column is NA
     */
    public boolean isNA(int index) {
        return get(index) == Values.NA || get(index) == null;
    }

    @Override
    public <C> C get(T headerName, Class<C> cl) {
        Object value = get(headerName);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no " + cl.getName() + " value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public <C> C getOrNull(T headerName, Class<C> cl) {
        Object value = get(headerName);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public <C> C get(int index, Class<C> cl) {
        Object value = get(index);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no " + cl.getName() + " value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public <C> C getOrNull(int index, Class<C> cl) {
        Object value = get(index);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o){
        if(o == null || !(this.getClass().equals(o.getClass()))){
            return false;
        }
        if(o == this){
            return true;
        }
        BasicRow<?,?,?> r = (BasicRow<?,?,?>)o;
        for(int i = 0; i < values.length; i++){
            if(values[i] == null && r.values[i] == null){
                continue;
            }
            if(values[i] == null || r.values[i] == null){
                return false;
            }
            if(!values[i].equals(r.values[i])){
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("] ");
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i]);
            if (i < values.length - 1) {
                sb.append("\t");
            }
        }
        return sb.toString();
    }
}
