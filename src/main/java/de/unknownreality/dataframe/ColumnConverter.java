package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 22.06.2016.
 */
public class ColumnConverter {

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

    private ColumnConverter(Map<Class<?>, Class<? extends DataFrameColumn>> typesMap) {
        this.columnTypesMap.putAll(typesMap);
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> Class<C> getColumnType(Class<T> type) {
        Class<? extends DataFrameColumn> columnType = columnTypesMap.get(type);
        if (columnType == null) {
            return null;
        }
        return (Class<C>) columnType;
    }

    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> ColumnConverter addType(Class<T> type, Class<C> columnType) {
        columnTypesMap.put(type, columnType);
        return this;
    }


    public static ColumnConverter create() {
        return new ColumnConverter(DEFAULT_COLUMN_TYPES);
    }


}
