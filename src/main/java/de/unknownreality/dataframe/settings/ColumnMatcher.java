package de.unknownreality.dataframe.settings;

import de.unknownreality.dataframe.DataFrameColumn;

@FunctionalInterface
public interface ColumnMatcher {
    boolean match(DataFrameColumn<?, ?> column);
}