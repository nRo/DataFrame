package de.unknownreality.dataframe.type.impl;

import de.unknownreality.dataframe.column.settings.ColumnSettings;
import de.unknownreality.dataframe.type.ValueType;

import java.util.Comparator;

public abstract class ComparableType<T extends Comparable<T>> extends ValueType<T> {
    private final Comparator<T> defaultComparator = (o1, o2) -> {
        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        return o1.compareTo(o2);
    };

    public ComparableType(ColumnSettings columnSettings) {
        super(columnSettings);
    }

    @Override
    public Comparator<T> getComparator() {
        return defaultComparator;
    }
}
