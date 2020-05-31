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

package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.type.ValueType;

/**
 * Created by Alex on 15.03.2016.
 */
public class GroupValues implements Row<Object, String> {
    private final Object[] values;
    private final GroupHeader groupHeader;

    public GroupValues(Object[] groupValues, GroupHeader header) {
        this.values = groupValues;
        this.groupHeader = header;
    }

    @Override
    public ValueType getType(int index) {
        return groupHeader.getValueType(index);
    }

    @Override
    public ValueType getType(String headerName) {
        return groupHeader.getValueType(headerName);
    }

    /**
     * Returns the group values as {@link Comparable} array
     *
     * @return values array
     */
    public Object[] getValues() {
        return values;
    }

    @Override
    public Object get(String headerName) {
        int index = groupHeader.getIndex(headerName);
        if (index == -1) {
            throw new DataFrameRuntimeException(String.format("group header name not found '%s'", headerName));
        }
        return get(index);
    }

    @Override
    public Object get(int index) {
        return this.values[index];
    }

    @Override
    public Double getDouble(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(get(index)).doubleValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no double value in group col " + index + " (" + value + ")");
        }
    }

    @Override
    public String getString(int index) {
        Object value = get(index);
        if (value != null) {
            return value.toString();
        }
        throw new DataFrameRuntimeException("no String value in group col " + index + " (null)");
    }

    @Override
    public Boolean getBoolean(int index) {
        Object value = get(index);
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        throw new DataFrameRuntimeException("no boolean value in group col " + index + " (" + value + ")");
    }

    @Override
    public Double getDouble(String name) {
        Object value = get(name);
        try {
            return Number.class.cast(get(name)).doubleValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no double value in group col " + name + " (" + value + ")");
        }
    }

    @Override
    public String getString(String name) {
        return getString(groupHeader.getIndex(name));
    }

    @Override
    public Boolean getBoolean(String name) {
        return getBoolean(groupHeader.getIndex(name));

    }

    @Override
    public Integer getInteger(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(get(index)).intValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no int value in group col " + index + " (" + value + ")");
        }
    }

    @Override
    public Integer getInteger(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).intValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no int value in group col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no float value in group col " + index + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no float value in col " + headerName + " (" + value + ")");
        }
    }


    @Override
    public <T> T get(String headerName, Class<T> cl) {
        Object value = get(headerName);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no " + cl.getName() + " value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public <T> T getOrNull(String headerName, Class<T> cl) {
        Object value = get(headerName);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public <T> T get(int index, Class<T> cl) {
        Object value = get(index);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no " + cl.getName() + " value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public <T> T getOrNull(int index, Class<T> cl) {
        Object value = get(index);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            return null;
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
    public Long getLong(String headerName) {
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
    public Short getShort(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).shortValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no short value in col " + headerName + " (" + value + ")");
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
    public Byte getByte(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).byteValue();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("no byte value in col " + headerName + " (" + value + ")");
        }
    }


    /**
     * Returns the number of group values
     *
     * @return number of  group values
     */
    @Override
    public int size() {
        return values.length;
    }
}
