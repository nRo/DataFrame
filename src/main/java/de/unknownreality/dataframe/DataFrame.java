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
 * Created by algru on 12.06.2017.
 */
public interface DataFrame extends DataContainer<DataFrameHeader, DataRow> {
    DataFrame setPrimaryKey(String... colNames);

    DataFrame setPrimaryKey(DataFrameColumn... cols);

    DataFrame removePrimaryKey();

    DataFrame removeIndex(String name);

    DataFrame renameColumn(String name, String newName);

    @SuppressWarnings("unchecked")
    DataFrame addColumn(DataFrameColumn column);

    <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name);

    @SuppressWarnings("unchecked")
    <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap);

    <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap, ColumnAppender<T> appender);

    <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame addColumn(Class<C> type, String name, ColumnAppender<T> appender);

    DataFrame addColumns(Collection<DataFrameColumn> columns);

    DataFrame addColumns(DataFrameColumn... columns);

    DataFrame append(Comparable... values);

    @SuppressWarnings("unchecked")
    DataFrame append(DataRow row);

    DataFrame update(DataRow dataRow);

    DataFrame set(Collection<DataRow> rows);

    DataFrame set(DataFrameHeader header, Collection<DataRow> rows);

    DataFrame removeColumn(String header);

    DataFrame removeColumn(DataFrameColumn column);

    DataFrame sort(SortColumn... columns);

    DataFrame sort(Comparator<DataRow> comp);

    DataFrame sort(String name);

    DataFrame sort(String name, SortColumn.Direction dir);

    DataFrame shuffle();

    DataFrame select(String colName, Comparable value);

    DataRow selectFirst(String colName, Comparable value);

    DataRow selectFirst(String predicateString);

    DataRow selectFirst(FilterPredicate predicate);

    DataFrame select(FilterPredicate predicate);

    DataFrame select(String predicateString);


    DataFrame filter(String predicateString);

    DataFrame filter(FilterPredicate predicate);


    List<DataRow> selectRows(String predicateString);

    List<DataRow> selectRows(FilterPredicate predicate);


    DataFrame transform(DataFrameTransform transformer);

    DataRow findByPrimaryKey(Comparable... keyValues);

    DataFrame reverse();

    DataFrame addIndex(String indexName, String... columnNames);

    DataFrame addIndex(String indexName, DataFrameColumn... columns);

    int size();

    DataFrame subset(int from, int to);

    DataFrame createSubset(int from, int to);

    List<DataRow> getRows(int from, int to);

    List<DataRow> getRows();

    DataFrameHeader getHeader();

    DataFrame concat(DataFrame other);

    DataFrame concat(Collection<DataFrame> dataFrames);

    DataFrame concat(DataFrame... dataFrames);

    boolean isCompatible(DataFrame input);

    DataRow getRow(int i);

    Comparable[] getRowValues(int i);

    Collection<String> getColumnNames();

    <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrameColumn<T,C> getColumn(String name);

    <T extends DataFrameColumn> T getColumn(String name, Class<T> cl);

    <T extends Number & Comparable<T>, C extends NumberColumn<T, C>> NumberColumn<T,C> getNumberColumn(String name);

    StringColumn getStringColumn(String name);

    DoubleColumn getDoubleColumn(String name);

    IntegerColumn getIntegerColumn(String name);

    FloatColumn getFloatColumn(String name);

    BooleanColumn getBooleanColumn(String name);

    ByteColumn getByteColumn(String name);

    LongColumn getLongColumn(String name);

    ShortColumn getShortColumn(String name);

    DataGrouping groupBy(String... column);

    JoinedDataFrame joinLeft(DataFrame dataFrame, String... joinColumns);

    JoinedDataFrame joinLeft(DataFrame dataFrame, JoinColumn... joinColumns);

    JoinedDataFrame joinLeft(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns);

    JoinedDataFrame joinRight(DataFrame dataFrame, String... joinColumns);

    JoinedDataFrame joinRight(DataFrame dataFrame, JoinColumn... joinColumns);

    JoinedDataFrame joinRight(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns);

    JoinedDataFrame joinInner(DataFrame dataFrame, String... joinColumns);

    JoinedDataFrame joinInner(DataFrame dataFrame, JoinColumn... joinColumns);

    JoinedDataFrame joinInner(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns);

    DataFrame copy();

    boolean containsColumn(DataFrameColumn column);

    boolean isIndexColumn(DataFrameColumn column);

    List<DataRow> findByIndex(String name, Comparable... values);

    DataRow findFirstByIndex(String name, Comparable... values);

    Collection<DataFrameColumn> getColumns();

    public Iterable<? extends DataRow> rows();
}
