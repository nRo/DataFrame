package de.unknownreality.dataframe;

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.column.*;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.index.Index;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinedDataFrame;
import de.unknownreality.dataframe.sort.SortColumn;
import de.unknownreality.dataframe.transform.DataFrameTransform;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public interface DataFrameProxy extends DataFrame {
    DataFrame getDataFrame();

    @Override
    default String getName() {
        return getDataFrame().getName();
    }

    @Override
    default void setName(String name) {
        getDataFrame().setName(name);
    }

    @Override
    default int getVersion() {
        return getDataFrame().getVersion();
    }

    @Override
    default DataFrame setPrimaryKey(String... colNames) {
        return getDataFrame().setPrimaryKey(colNames);
    }

    @Override
    default DataFrame setPrimaryKey(DataFrameColumn... cols) {
        return getDataFrame().setPrimaryKey(cols);
    }

    @Override
    default DataFrame removePrimaryKey() {
        return getDataFrame().removePrimaryKey();
    }

    @Override
    default DataFrame removeIndex(String name) {
        return getDataFrame().removeIndex(name);
    }

    @Override
    default DataFrame renameColumn(String name, String newName) {
        return getDataFrame().renameColumn(name, newName);
    }

    @Override
    default ColumnSelection selectColumns(String... columnNames) {
        return getDataFrame().selectColumns(columnNames);
    }

    @Override
    default ColumnSelection selectColumns(DataFrameColumn... columns) {
        return getDataFrame().selectColumns(columns);
    }

    @Override
    default DataFrame addColumn(DataFrameColumn column) {
        return getDataFrame().addColumn(column);
    }

    @Override
    default <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name) {
        return getDataFrame().addColumn(type, name);
    }

    @Override
    default <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap) {
        return getDataFrame().addColumn(type, name, columnTypeMap);
    }

    @Override
    default <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap, ColumnAppender<T> appender) {
        return getDataFrame().addColumn(type, name, columnTypeMap, appender);
    }

    @Override
    default <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame addColumn(Class<C> type, String name, ColumnAppender<T> appender) {
        return getDataFrame().addColumn(type, name, appender);
    }

    @Override
    default DataFrame addBooleanColumn(String name) {
        return getDataFrame().addBooleanColumn(name);
    }

    @Override
    default DataFrame addByteColumn(String name) {
        return getDataFrame().addByteColumn(name);
    }

    @Override
    default DataFrame addDoubleColumn(String name) {
        return getDataFrame().addDoubleColumn(name);
    }

    @Override
    default DataFrame addFloatColumn(String name) {
        return getDataFrame().addFloatColumn(name);
    }

    @Override
    default DataFrame addIntegerColumn(String name) {
        return getDataFrame().addIntegerColumn(name);
    }

    @Override
    default DataFrame addLongColumn(String name) {
        return getDataFrame().addLongColumn(name);
    }

    @Override
    default DataFrame addShortColumn(String name) {
        return getDataFrame().addShortColumn(name);
    }

    @Override
    default DataFrame addStringColumn(String name) {
        return getDataFrame().addStringColumn(name);
    }

    @Override
    default DataFrame addColumns(Collection<DataFrameColumn> columns) {
        return getDataFrame().addColumns(columns);
    }

    @Override
    default DataFrame addColumns(DataFrameColumn... columns) {
        return getDataFrame().addColumns(columns);
    }

    @Override
    default DataFrame replaceColumn(DataFrameColumn existing, DataFrameColumn replacement) {
        return getDataFrame().replaceColumn(existing, replacement);
    }

    @Override
    default DataFrame replaceColumn(String existing, DataFrameColumn replacement) {
        return getDataFrame().replaceColumn(existing, replacement);
    }

    @Override
    default DataFrame append(DataFrame dataFrame, int rowIndex) {
        return getDataFrame().append(dataFrame, rowIndex);
    }

    @Override
    default DataFrame append(Comparable... values) {
        return getDataFrame().append(values);
    }

    @Override
    default DataFrame append(DataRow row) {
        return getDataFrame().append(row);
    }

    @Override
    default DataFrame appendMatchingRow(DataRow row) {
        return getDataFrame().append(row);
    }

    @Override
    default DataFrame update(DataRow dataRow) {
        return getDataFrame().update(dataRow);
    }

    @Override
    default DataFrame set(DataFrameHeader header) {
        return getDataFrame().set(header);
    }

    @Override
    default DataFrame set(DataRows rows) {
        return getDataFrame().set(rows);
    }

    @Override
    default DataFrame removeColumn(String header) {
        return getDataFrame().removeColumn(header);
    }

    @Override
    default DataFrame removeColumn(DataFrameColumn column) {
        return getDataFrame().removeColumn(column);
    }

    @Override
    default DataFrame sort(SortColumn... columns) {
        return getDataFrame().sort(columns);
    }

    @Override
    default DataFrame sort(Comparator<DataRow> comp) {
        return getDataFrame().sort(comp);
    }

    @Override
    default DataFrame sort(String name) {
        return getDataFrame().sort(name);
    }

    @Override
    default DataFrame sort(String name, SortColumn.Direction dir) {
        return getDataFrame().sort(name, dir);
    }

    @Override
    default DataFrame shuffle() {
        return getDataFrame().shuffle();
    }

    @Override
    default DataFrame select(String colName, Comparable value) {
        return getDataFrame().select(colName, value);
    }

    @Override
    default DataRow selectFirst(String colName, Comparable value) {
        return getDataFrame().selectFirst(colName, value);
    }

    @Override
    default DataRow selectFirst(String predicateString) {
        return getDataFrame().selectFirst(predicateString);
    }

    @Override
    default DataRow selectFirst(FilterPredicate predicate) {
        return getDataFrame().selectFirst(predicate);
    }

    @Override
    default DataFrame select(FilterPredicate predicate) {
        return getDataFrame().select(predicate);
    }

    @Override
    default DataFrame select(String predicateString) {
        return getDataFrame().select(predicateString);
    }

    @Override
    default DataFrame filter(String predicateString) {
        return getDataFrame().filter(predicateString);
    }

    @Override
    default DataFrame filter(FilterPredicate predicate) {
        return getDataFrame().filter(predicate);
    }

    @Override
    default DataRows selectRows(String colName, Comparable value) {
        return getDataFrame().selectRows(colName, value);
    }

    @Override
    default DataRows selectRows(String predicateString) {
        return getDataFrame().selectRows(predicateString);
    }

    @Override
    default DataRows selectRows(FilterPredicate predicate) {
        return getDataFrame().selectRows(predicate);
    }

    @Override
    default DataFrame transform(DataFrameTransform transformer) {
        return getDataFrame().transform(transformer);
    }

    @Override
    default DataRow selectByPrimaryKey(Comparable... keyValues) {
        return getDataFrame().selectByPrimaryKey(keyValues);
    }

    @Override
    default DataFrame reverse() {
        return getDataFrame().reverse();
    }

    @Override
    default DataFrame addIndex(String indexName, String... columnNames) {
        return getDataFrame().addIndex(indexName, columnNames);
    }

    @Override
    default DataFrame addIndex(String indexName, DataFrameColumn... columns) {
        return getDataFrame().addIndex(indexName, columns);
    }

    @Override
    default DataFrame addIndex(Index index) {
        return getDataFrame().addIndex(index);
    }

    @Override
    default int size() {
        return getDataFrame().size();
    }

    @Override
    default boolean isEmpty() {
        return getDataFrame().isEmpty();
    }

    @Override
    default DataFrame subset(int from, int to) {
        return getDataFrame().subset(from, to);
    }

    @Override
    default DataFrame filterSubset(int from, int to) {
        return getDataFrame().filterSubset(from, to);
    }

    @Override
    default DataFrame selectSubset(int from, int to) {
        return getDataFrame().selectSubset(from, to);
    }

    @Override
    default DataRows getRows(int from, int to) {
        return getDataFrame().getRows(from, to);
    }

    @Override
    default DataRows getRows() {
        return getDataFrame().getRows();
    }

    @Override
    default DataFrameHeader getHeader() {
        return getDataFrame().getHeader();
    }

    @Override
    default <T> List<T> map(Class<T> cl) {
        return getDataFrame().map(cl);
    }

    @Override
    default DataFrame concat(DataFrame other) {
        return getDataFrame().concat(other);
    }

    @Override
    default DataFrame concat(Collection<DataFrame> dataFrames) {
        return getDataFrame().concat(dataFrames);
    }

    @Override
    default DataFrame concat(DataFrame... dataFrames) {
        return getDataFrame().concat(dataFrames);
    }

    @Override
    default boolean isCompatible(DataFrame input) {
        return getDataFrame().isCompatible(input);
    }

    @Override
    default DataRow getRow(int i) {
        return getDataFrame().getRow(i);
    }

    @Override
    default Collection<String> getColumnNames() {
        return getDataFrame().getColumnNames();
    }

    @Override
    default <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrameColumn<T, C> getColumn(String name) {
        return getDataFrame().getColumn(name);
    }

    @Override
    default <T extends DataFrameColumn> T getColumn(String name, Class<T> cl) {
        return getDataFrame().getColumn(name, cl);
    }

    @Override
    default <T extends Number & Comparable<T>, C extends NumberColumn<T, C>> NumberColumn<T, C> getNumberColumn(String name) {
        return getDataFrame().getNumberColumn(name);
    }

    @Override
    default StringColumn getStringColumn(String name) {
        return getDataFrame().getStringColumn(name);
    }

    @Override
    default DoubleColumn getDoubleColumn(String name) {
        return getDataFrame().getDoubleColumn(name);
    }

    @Override
    default IntegerColumn getIntegerColumn(String name) {
        return getDataFrame().getIntegerColumn(name);
    }

    @Override
    default FloatColumn getFloatColumn(String name) {
        return getDataFrame().getFloatColumn(name);
    }

    @Override
    default BooleanColumn getBooleanColumn(String name) {
        return getDataFrame().getBooleanColumn(name);
    }

    @Override
    default ByteColumn getByteColumn(String name) {
        return getDataFrame().getByteColumn(name);
    }

    @Override
    default LongColumn getLongColumn(String name) {
        return getDataFrame().getLongColumn(name);
    }

    @Override
    default ShortColumn getShortColumn(String name) {
        return getDataFrame().getShortColumn(name);
    }

    @Override
    default DataGrouping groupBy(String... column) {
        return getDataFrame().groupBy(column);
    }

    @Override
    default JoinedDataFrame joinLeft(DataFrame dataFrame, String... joinColumns) {
        return getDataFrame().joinLeft(dataFrame, joinColumns);
    }

    @Override
    default JoinedDataFrame joinLeft(DataFrame dataFrame, JoinColumn... joinColumns) {
        return getDataFrame().joinLeft(dataFrame, joinColumns);
    }

    @Override
    default JoinedDataFrame joinLeft(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return getDataFrame().joinLeft(dataFrame, suffixA, suffixB, joinColumns);
    }

    @Override
    default JoinedDataFrame joinRight(DataFrame dataFrame, String... joinColumns) {
        return getDataFrame().joinRight(dataFrame, joinColumns);
    }

    @Override
    default JoinedDataFrame joinRight(DataFrame dataFrame, JoinColumn... joinColumns) {
        return getDataFrame().joinRight(dataFrame, joinColumns);
    }

    @Override
    default JoinedDataFrame joinRight(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return getDataFrame().joinRight(dataFrame, suffixA, suffixB, joinColumns);
    }

    @Override
    default JoinedDataFrame joinInner(DataFrame dataFrame, String... joinColumns) {
        return getDataFrame().joinInner(dataFrame, joinColumns);
    }

    @Override
    default JoinedDataFrame joinInner(DataFrame dataFrame, JoinColumn... joinColumns) {
        return getDataFrame().joinInner(dataFrame, joinColumns);
    }

    @Override
    default JoinedDataFrame joinInner(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return getDataFrame().joinInner(dataFrame, suffixA, suffixB, joinColumns);
    }

    @Override
    default DataFrame copy() {
        return getDataFrame().copy();
    }

    @Override
    default boolean containsColumn(DataFrameColumn column) {
        return getDataFrame().containsColumn(column);
    }

    @Override
    default boolean isIndexColumn(DataFrameColumn column) {
        return getDataFrame().isIndexColumn(column);
    }

    @Override
    default DataRows selectRowsByIndex(String name, Comparable... values) {
        return getDataFrame().selectRowsByIndex(name, values);
    }

    @Override
    default DataRow selectFirstRowByIndex(String name, Comparable... values) {
        return getDataFrame().selectFirstRowByIndex(name, values);
    }

    @Override
    default DataFrame selectByIndex(String name, Comparable... values) {
        return getDataFrame().selectByIndex(name, values);
    }

    @Override
    default Collection<DataFrameColumn> getColumns() {
        return getDataFrame().getColumns();
    }

    @Override
    default Iterable<? extends DataRow> rows() {
        return getDataFrame().rows();
    }

    @Override
    default Comparable getValue(int col, int row) {
        return getDataFrame().getValue(col, row);
    }

    @Override
    default void setValue(int col, int row, Comparable newValue) {
        getDataFrame().setValue(col, row, newValue);

    }

    @Override
    default boolean isNA(int col, int row) {
        return getDataFrame().isNA(col, row);
    }

    @Override
    default void clear() {
        getDataFrame().clear();

    }

    @Override
    default Iterator<DataRow> iterator() {
        return getDataFrame().iterator();
    }
}
