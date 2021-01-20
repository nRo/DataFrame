package de.unknownreality.dataframe.type.impl;

import de.unknownreality.dataframe.column.settings.ColumnSettings;
import de.unknownreality.dataframe.common.NumberUtil;

public abstract class NumberType<T extends Number & Comparable<T>> extends ComparableType<T> {
    public NumberType(ColumnSettings columnSettings) {
        super(columnSettings);
    }

    @Override
    public T convertRaw(Object o) {
        if (o instanceof Number) {
            return NumberUtil.convert((Number) o, getType());
        }
        return super.convertRaw(o);
    }

    @Override
    public int compareRaw(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return NumberUtil.compare((Number) a, (Number) b);
        }
        return super.compareRaw(a, b);
    }
}
