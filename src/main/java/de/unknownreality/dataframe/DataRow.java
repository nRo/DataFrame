package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.Header;
import de.unknownreality.dataframe.common.Row;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataRow implements Row<Comparable> {
    private final Comparable[] values;
    private final Header<String> header;
    private final int index;

    public DataRow(Header<String> header, Comparable[] values, int index) {
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

    public Comparable get(String headerName) {
        int index = header.getIndex(headerName);
        if (index == -1) {
            throw new DataFrameRuntimeException(String.format("header name not found '%s'", headerName));
        }
        return this.values[index];
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
            throw new DataFrameRuntimeException("no double value in col " + index + " (" + value + ")");
        }
    }

    /**
     * Returns the values of this row as {@link Comparable} array
     *
     * @return values array
     */
    public Comparable[] getValues() {
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
    public Double getDouble(String name) {
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
    public Number getNumber(String name) {
        return getNumber(header.getIndex(name));
    }


    @Override
    public String getString(String name) {
        return getString(header.getIndex(name));
    }

    @Override
    public Boolean getBoolean(String name) {
        return getBoolean(header.getIndex(name));

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
    public Integer getInteger(String headerName) {
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
    public Float getFloat(String headerName) {
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
    public Byte getByte(String headerName) {
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
    public boolean isNA(String headerName) {
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
