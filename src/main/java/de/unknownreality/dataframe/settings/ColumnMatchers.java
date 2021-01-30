package de.unknownreality.dataframe.settings;

import de.unknownreality.dataframe.DataFrameColumn;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ColumnMatchers {

    public ColumnMatcher all() {
        return (c) -> true;
    }

    public static ColumnMatcher byName(String... names) {
        final Set<String> nameSet = new HashSet<>(Arrays.asList(names));
        return (c) -> nameSet.contains(c.getName());
    }

    public static ColumnMatcher byIndex(Integer... columnIndices) {
        final Set<Integer> idxSet = new HashSet<>(Arrays.asList(columnIndices));
        return (c) -> idxSet.contains(c.getColumnIndex());
    }

    public static ColumnMatcher byValueType(Class<?>... types) {
        final Set<Class<?>> clSet = new HashSet<>(Arrays.asList(types));
        return (c) -> {
            return clSet.contains(c.getValueType().getType()) ||
                    clSet.contains(c.getValueType().getClass());
        };
    }

    @SafeVarargs
    public static ColumnMatcher byColumnType(Class<? extends DataFrameColumn<?, ?>>... types) {
        final Set<Class<?>> clSet = new HashSet<>(Arrays.asList(types));
        return (c) -> clSet.contains(c.getClass());
    }

    public static ColumnMatcher all(ColumnMatcher... matchers) {
        return (c) -> {
            for (ColumnMatcher matcher : matchers) {
                if (matcher.match(c)) {
                    return true;
                }
            }
            return false;
        };
    }
}