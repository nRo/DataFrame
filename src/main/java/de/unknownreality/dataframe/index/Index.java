package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DataFrameColumn;

import java.util.List;

/**
 * Created by Alex on 27.05.2016.
 */
public interface Index {
    void update(DataRow dataRow);

    void remove(DataRow dataRow);

    int find(Comparable... values);

    String getName();

    boolean containsColumn(DataFrameColumn column);

    List<DataFrameColumn> getColumns();

    void clear();

}
