package de.unknownreality.data.frame.group;

import de.unknownreality.data.common.Row;

/**
 * Created by Alex on 15.03.2016.
 */
public class GroupValues implements Row<Comparable> {
    private Comparable[] values;
    private GroupHeader groupHeader;

    public GroupValues(Comparable[] groupValues,GroupHeader header){
        this.values = groupValues;
        this.groupHeader = header;
    }

    public Comparable[] getValues() {
        return values;
    }

    public Comparable get(String headerName) {
        int index = groupHeader.getIndex(headerName);
        if (index == -1) {
            throw new IllegalArgumentException(String.format("group header name not found '%s'", headerName));
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
            throw new IllegalArgumentException("no double value in group col " + index + " (" + value + ")");
        }
    }

    public String getString(int index) {
        Object value = get(index);
        if (value != null) {
            return value.toString();
        }
        throw new IllegalArgumentException("no String value in group col " + index + " (" + value + ")");
    }

    public Boolean getBoolean(int index) {
        Object value = get(index);
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        throw new IllegalArgumentException("no boolean value in group col " + index + " (" + value + ")");
    }


    public Double getDouble(String name) {
        Object value = get(name);
        try {
            return Number.class.cast(get(name)).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no double value in group col " + name + " (" + value + ")");
        }
    }

    public String getString(String name) {
        return getString(groupHeader.getIndex(name));
    }

    public Boolean getBoolean(String name) {
        return getBoolean(groupHeader.getIndex(name));

    }

    @Override
    public Integer getInteger(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(get(index)).intValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no int value in group col " + index + " (" + value + ")");
        }
    }

    @Override
    public Integer getInteger(String headerName) {
        Object value = get(headerName);
        try {
            return Number.class.cast(value).intValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no int value in group col " + headerName + " (" + value + ")");
        }
    }

    @Override
    public Float getFloat(int index) {
        Object value = get(index);
        try {
            return Number.class.cast(value).floatValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("no float value in group col " + index + " (" + value + ")");
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
    public<T> T get(String headerName, Class<T> cl) {
        Object value = get(headerName);
        try {
            return cl.cast(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("no "+cl.getName()+" value in col " + headerName + " (" + value + ")");
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

    public int size(){
        return values.length;
    }
}
