package de.unknownreality.dataframe;

/**
 * Created by Alex on 13.03.2016.
 */
public interface ColumnAppender<T extends Comparable<T>> {
    T createRowValue(DataRow row);
}
