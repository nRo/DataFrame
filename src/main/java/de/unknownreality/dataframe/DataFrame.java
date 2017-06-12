package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.*;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinedDataFrame;
import de.unknownreality.dataframe.sort.SortColumn;
import de.unknownreality.dataframe.transform.DataFrameTransform;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created by algru on 11.06.2017.
 */
public interface DataFrame<H extends DataFrameHeader<H>, R extends DataRow> extends DataContainer<H, R> {

    DataFrame<H,R> setPrimaryKey(String... colNames);

    DataFrame<H,R> setPrimaryKey(DataFrameColumn... cols);

    DataFrame<H,R> removePrimaryKey();

    DataFrame<H,R> removeIndex(String name);

    DataFrame<H,R> renameColumn(String name, String newName);

    @SuppressWarnings("unchecked")
    DataFrame<H,R> addColumn(DataFrameColumn column);

    <T extends Comparable<T>> DataFrame<H,R> addColumn(Class<T> type, String name);

    @SuppressWarnings("unchecked")
    <T extends Comparable<T>> DataFrame<H,R> addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap);

    <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame<H,R> addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap, ColumnAppender<T> appender);

    <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame<H,R> addColumn(Class<C> type, String name, ColumnAppender<T> appender);

    DataFrame<H,R> addColumns(Collection<DataFrameColumn> columns);

    DataFrame<H,R> addColumns(DataFrameColumn... columns);

    DataFrame<H,R> append(Comparable... values);

    @SuppressWarnings("unchecked")
    DataFrame<H,R> append(R row);

    DataFrame<H,R> update(R dataRow);

    DataFrame<H,R> set(Collection<R> rows);

    DataFrame<H,R> set(H header, Collection<R> rows);

    DataFrame<H,R> removeColumn(String header);

    DataFrame<H,R> removeColumn(DataFrameColumn column);

    DataFrame<H,R> sort(SortColumn... columns);

    DataFrame<H,R> sort(Comparator<DataRow> comp);

    DataFrame<H,R> sort(String name);

    DataFrame<H,R> sort(String name, SortColumn.Direction dir);

    DataFrame<H,R> shuffle();

    DataFrame<H,R> select(String colName, Comparable value);

    R selectFirst(String colName, Comparable value);

    R selectFirst(String predicateString);

    R selectFirst(FilterPredicate predicate);

    DataFrame<H,R> select(FilterPredicate predicate);

    DataFrame<H,R> select(String predicateString);


    DataFrame<H,R> filter(String predicateString);

    DataFrame<H,R> filter(FilterPredicate predicate);


    List<R> selectRows(String predicateString);

    List<R> selectRows(FilterPredicate predicate);

    DataFrame<H,R> transform(DataFrameTransform transformer);

    R findByPrimaryKey(Comparable... keyValues);

    DataFrame<H,R> reverse();

    DataFrame<H,R> addIndex(String indexName, String... columnNames);

    DataFrame<H,R> addIndex(String indexName, DataFrameColumn... columns);

    int size();

    DataFrame<H,R> subset(int from, int to);

    DataFrame<H,R> createSubset(int from, int to);

    List<R> getRows(int from, int to);

    List<R> getRows();

    H getHeader();

    DataFrame<H,R> concat(DataFrame<H,R> other);

    DataFrame<H,R> concat(Collection<DataFrame<H,R>> dataFrames);

    DataFrame<H,R> concat(DataFrame<H,R>... dataFrames);

    boolean isCompatible(DataFrame<H,R> input);

    R getRow(int i);

    Comparable[] getRowValues(int i);

    Collection<String> getColumnNames();

    DataFrameColumn getColumn(String name);

    <T extends DataFrameColumn> T getColumn(String name, Class<T> cl);

    NumberColumn getNumberColumn(String name);

    StringColumn getStringColumn(String name);

    DoubleColumn getDoubleColumn(String name);

    IntegerColumn getIntegerColumn(String name);

    FloatColumn getFloatColumn(String name);

    BooleanColumn getBooleanColumn(String name);

    ByteColumn getByteColumn(String name);

    LongColumn getLongColumn(String name);

    ShortColumn getShortColumn(String name);

    DataGrouping groupBy(String... column);

    JoinedDataFrame joinLeft(DataFrame<H,R> dataFrame, String... joinColumns);

    JoinedDataFrame joinLeft(DataFrame<H,R> dataFrame, JoinColumn... joinColumns);

    JoinedDataFrame joinLeft(DataFrame<H,R> dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns);

    JoinedDataFrame joinRight(DataFrame<H,R> dataFrame, String... joinColumns);

    JoinedDataFrame joinRight(DataFrame<H,R> dataFrame, JoinColumn... joinColumns);

    JoinedDataFrame joinRight(DataFrame<H,R> dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns);

    JoinedDataFrame joinInner(DataFrame<H,R> dataFrame, String... joinColumns);

    JoinedDataFrame joinInner(DataFrame<H,R> dataFrame, JoinColumn... joinColumns);

    JoinedDataFrame joinInner(DataFrame<H,R> dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns);

    DataFrame<H,R> copy();

    boolean containsColumn(DataFrameColumn column);

    boolean isIndexColumn(DataFrameColumn column);

    List<R> findByIndex(String name, Comparable... values);

    R findFirstByIndex(String name, Comparable... values);

    Collection<DataFrameColumn> getColumns();

    void notifyColumnValueChanged(DataFrameColumn column, int index, Comparable value);

    void notifyColumnChanged(DataFrameColumn column);

    @Override
    Iterator<R> iterator();
}
