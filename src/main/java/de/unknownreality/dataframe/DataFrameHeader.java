package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameHeader extends BasicHeader {
    private static Logger log = LoggerFactory.getLogger(DataFrameHeader.class);
    private final Map<String, Integer> headerMap = new HashMap<>();
    private final List<String> headers = new ArrayList<>();
    private final Map<String, Class<? extends Comparable>> typesMap = new HashMap<>();
    private final Map<String, Class<? extends DataFrameColumn>> colTypeMap = new HashMap<>();

    public int size() {
        return headers.size();
    }

    public DataFrameHeader add(DataFrameColumn<?, ?> column) {
        return add(column.getName(), column.getClass(), column.getType());
    }

    public DataFrameHeader add(String name, Class<? extends DataFrameColumn> colClass, Class<? extends Comparable> type) {
        int index = headers.size();
        headers.add(name);
        headerMap.put(name, index);
        typesMap.put(name, type);
        colTypeMap.put(name, colClass);
        return this;
    }


    public void remove(String name) {
        boolean fix = false;
        for (String s : headers) {
            if (!fix && s.equals(name)) {
                fix = true;
                continue;
            }
            if (fix) {
                headerMap.put(s, headerMap.get(s) - 1);
            }
        }
        headers.remove(name);
        headerMap.remove(name);
        typesMap.remove(name);
        colTypeMap.remove(name);
    }

    public void rename(String oldName, String newName) {
        for (int i = 0; i < headers.size(); i++) {
            if (headers.get(i).equals(oldName)) {
                headers.set(i, newName);
                Class<? extends Comparable> type = typesMap.get(oldName);
                typesMap.remove(oldName);
                typesMap.put(newName, type);

                Class<? extends DataFrameColumn> colType = colTypeMap.get(oldName);
                colTypeMap.remove(oldName);
                colTypeMap.put(newName, colType);

                Integer index = headerMap.get(oldName);
                headerMap.remove(oldName);
                headerMap.put(newName, index);
                return;
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other.getClass() != this.getClass()) {
            return false;
        }
        DataFrameHeader otherHeader = (DataFrameHeader) other;
        if (size() != otherHeader.size()) {
            return false;
        }
        for (String s : headers) {
            if (!otherHeader.contains(s)) {
                return false;
            }
            if (getType(s) != otherHeader.getType(s)) {
                return false;
            }
        }
        return true;
    }

    public Class<? extends DataFrameColumn> getColumnType(String name) {
        return colTypeMap.get(name);
    }

    public Class<? extends DataFrameColumn> getColumnType(int index) {
        return colTypeMap.get(get(index));
    }

    public Class<? extends Comparable> getType(String name) {
        return typesMap.get(name);
    }

    public Class<? extends Comparable> getType(int index) {
        return typesMap.get(get(index));
    }

    public String get(int index) {
        if (index >= headers.size()) {
            throw new IllegalArgumentException(String.format("header index out of bounds %d > %d", index, (headers.size() - 1)));
        }
        return headers.get(index);
    }

    @Override
    public boolean contains(String value) {
        return headerMap.containsKey(value);
    }

    public void clear() {
        headerMap.clear();
        headers.clear();
        typesMap.clear();
    }


    public int getIndex(String name) {
        Integer index = headerMap.get(name);
        index = index == null ? -1 : index;
        return index;
    }


    public DataFrameHeader copy() {
        DataFrameHeader copy = new DataFrameHeader();
        for (String h : headers) {
            copy.add(h, getColumnType(h), getType(h));
        }
        return copy;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("#");
        for (int i = 0; i < headers.size(); i++) {
            sb.append(headers.get(i));
            if (i < headers.size() - 1) {
                sb.append("\t");
            }
        }
        return sb.toString();
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            int i = 0;

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported");
            }

            @Override
            public boolean hasNext() {
                return i != headers.size();
            }

            @Override
            public String next() {
                return headers.get(i++);
            }
        };
    }

}
