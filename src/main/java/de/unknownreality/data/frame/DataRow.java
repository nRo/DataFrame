package de.unknownreality.data.frame;

import de.unknownreality.data.common.Header;
import de.unknownreality.data.common.Row;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataRow implements Row<Comparable> {
    private Comparable[] values;
    private Header<String> header;
    private int index;
    public DataRow(Header<String> header, Comparable[] values, int index) {
        this.header = header;
        this.values = values;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public Comparable get(String headerName) {
        int index = header.getIndex(headerName);
        if (index == -1) {
            throw new IllegalArgumentException(String.format("header name not found '%s'", headerName));
        }
        return get(index);
    }

    public Comparable get(int index) {
        return this.values[index];
    }

    public Double getDouble(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(get(index)).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no double value in col " + index + " (" + value + ")");
        }
    }

    public String getString(int index) {
        Object value = get(index);
        if (value != null) {
            return value.toString();
        }
        throw new IllegalArgumentException("no String value in col " + index + " (" + value + ")");
    }

    public int size() {
        return values.length;
    }

    public Boolean getBoolean(int index) {
        Object value = get(index);
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        throw new IllegalArgumentException("no boolean value in col " + index + " (" + value + ")");
    }


    public Double getDouble(String name) {
        Object value = get(name);
        try {
            return Number.class.cast(get(name)).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no double value in col " + name + " (" + value + ")");
        }
    }

    public Number getNumber(int index) {
        Object value = get(index);
        if (value instanceof Number) {
            return (Number) value;
        }
        throw new IllegalArgumentException("no boolean value in col " + index + " (" + value + ")");
    }


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
            throw new IllegalArgumentException("no int value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Integer getInteger(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).intValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no int value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no float value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no float value in col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Long getLong(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).longValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no float value in col " + index + " (" + value + ")");
        }
    }

    @Override
    public Long getLong(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).longValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no float value in col " + headerName + " (" + value + ")");
        }
    }

    public boolean isNA(String headerName) {
        return get(headerName) == Values.NA || get(headerName) == null;
    }

    public boolean isNA(int index) {
        return get(index) == Values.NA || get(index) == null;
    }

    @Override
    public <T> T get(String headerName, Class<T> cl) {
        Object value = get(headerName);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("no " + cl.getName() + " value in col " + headerName + " (" + value + ")");
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
