package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Row;

/**
 * Created by Alex on 15.03.2016.
 */
public class GroupValues implements Row<Comparable> {
    private final Comparable[] values;
    private final GroupHeader groupHeader;

    public GroupValues(Comparable[] groupValues, GroupHeader header) {
        this.values = groupValues;
        this.groupHeader = header;
    }

    /**
     * Returns the group values as {@link Comparable} array
     *
     * @return values array
     */
    public Comparable[] getValues() {
        return values;
    }

    @Override
    public Comparable get(String headerName) {
        int index = groupHeader.getIndex(headerName);
        if (index == -1) {
            throw new DataFrameRuntimeException(String.format("group header name not found '%s'", headerName));
        }
        return get(index);
    }

    @Override
    public Comparable get(int index) {
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
     * Returns the number of values
     *
     * @return number of values
     */
    @Override
    public int size() {
        return values.length;
    }
}
