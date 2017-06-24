package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.*;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.io.*;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinedDataFrame;
import de.unknownreality.dataframe.sort.SortColumn;
import de.unknownreality.dataframe.transform.DataFrameTransform;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
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

    DataFrame replaceColumn(DataFrameColumn existing, DataFrameColumn replacement);

    DataFrame replaceColumn(String existing, DataFrameColumn replacement);

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

    <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrameColumn<T, C> getColumn(String name);

    <T extends DataFrameColumn> T getColumn(String name, Class<T> cl);

    <T extends Number & Comparable<T>, C extends NumberColumn<T, C>> NumberColumn<T, C> getNumberColumn(String name);

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

    static DataFrame load(File file, ReadFormat readFormat) {
        return DataFrameLoader.load(file, readFormat);

    }

    static DataFrame load(String content, ReadFormat readFormat) {
        return DataFrameLoader.load(content, readFormat);
    }

    static DataFrame load(String resource, ClassLoader classLoader, ReadFormat readFormat) {
        return DataFrameLoader.load(resource, classLoader, readFormat);

    }

    static DataFrame load(URL url, ReadFormat readFormat) {
        return DataFrameLoader.load(url, readFormat);

    }

    static DataFrame load(byte[] bytes, ReadFormat readFormat) {
        return DataFrameLoader.load(bytes, readFormat);

    }

    static DataFrame load(InputStream is, ReadFormat readFormat) {
        return DataFrameLoader.load(is, readFormat);
    }


    static DataFrame load(File file, DataReader reader) {
        return DataFrameLoader.load(file, reader);

    }

    static DataFrame load(String content, DataReader reader) {
        return DataFrameLoader.load(content, reader);
    }

    static DataFrame load(String resource, ClassLoader classLoader, DataReader reader) {
        return DataFrameLoader.load(resource, classLoader, reader);

    }

    static DataFrame load(URL url, DataReader reader) {
        return DataFrameLoader.load(url, reader);

    }

    static DataFrame load(byte[] bytes, DataReader reader) {
        return DataFrameLoader.load(bytes, reader);

    }

    static DataFrame load(InputStream is, DataReader reader) {
        return DataFrameLoader.load(is, reader);
    }

    static DataFrame load(DataIterator<?> dataIterator) {
        return DataFrameLoader.load(dataIterator);
    }

    static DataFrame load(DataIterator<?> dataIterator, FilterPredicate predicate) {
        return DataFrameLoader.load(dataIterator, predicate);
    }

    static DataFrame fromCSV(File file, char separator, boolean header) {
        return DataFrameLoader.fromCSV(file, separator, header);

    }

    static DataFrame fromCSV(String content, char separator, boolean header) {
        return DataFrameLoader.fromCSV(content, separator, header);
    }

    static DataFrame fromCSV(String resource, ClassLoader classLoader, char separator, boolean header) {
        return DataFrameLoader.fromCSV(resource, classLoader, separator, header);

    }

    static DataFrame fromCSV(URL url, char separator, boolean header) {
        return DataFrameLoader.fromCSV(url, separator, header);

    }

    static DataFrame fromCSV(byte[] bytes, char separator, boolean header) {
        return DataFrameLoader.fromCSV(bytes, separator, header);

    }

    static DataFrame fromCSV(InputStream is, char separator, boolean header) {
        return DataFrameLoader.fromCSV(is, separator, header);
    }

    static DataFrame fromCSV(File file, char separator, String headerPrefix) {
        return DataFrameLoader.fromCSV(file, separator, headerPrefix);

    }

    static DataFrame fromCSV(String content, char separator, String headerPrefix) {
        return DataFrameLoader.fromCSV(content, separator, headerPrefix);
    }

    static DataFrame fromCSV(String resource, ClassLoader classLoader, char separator, String headerPrefix) {
        return DataFrameLoader.fromCSV(resource, classLoader, separator, headerPrefix);

    }

    static DataFrame fromCSV(URL url, char separator, String headerPrefix) {
        return DataFrameLoader.fromCSV(url, separator, headerPrefix);

    }

    static DataFrame fromCSV(byte[] bytes, char separator, String headerPrefix) {
        return DataFrameLoader.fromCSV(bytes, separator, headerPrefix);

    }

    static DataFrame fromCSV(InputStream is, char separator, String headerPrefix) {
        return DataFrameLoader.fromCSV(is, separator, headerPrefix);
    }


    default void write(File file, DataWriter dataWriter) {
        DataFrameWriter.write(file, this, dataWriter);
    }

    default void write(File file, DataWriter dataWriter, boolean writeMetaFile) {
        DataFrameWriter.write(file, this, dataWriter, writeMetaFile);
    }

    default void write(Writer writer, DataWriter dataWriter) {
        DataFrameWriter.write(writer, this, dataWriter);
    }

    default void write(OutputStream outputStream, DataWriter dataWriter) {
        DataFrameWriter.write(outputStream, this, dataWriter);
    }

    default void write(File file, WriteFormat writeFormat) {
        DataFrameWriter.write(file, this, writeFormat);
    }

    default void write(File file, WriteFormat writeFormat, boolean writeMetaFile) {
        DataFrameWriter.write(file, this, writeFormat, writeMetaFile);
    }

    default void write(Writer writer, WriteFormat writeFormat) {
        DataFrameWriter.write(writer, this, writeFormat);
    }

    default void write(OutputStream outputStream, WriteFormat writeFormat) {
        DataFrameWriter.write(outputStream, this, writeFormat);
    }

    default void write(File file) {
        DataFrameWriter.write(file, this);
    }

    default void write(File file, boolean writeMetaFile) {
        DataFrameWriter.write(file, this, writeMetaFile);
    }

    default void write(Writer writer) {
        DataFrameWriter.write(writer, this);
    }

    default void write(OutputStream outputStream) {
        DataFrameWriter.write(outputStream, this);
    }

    default void writeCSV(File file, char separator, boolean writeHeader) {
        DataFrameWriter.writeCSV(file, this, separator, writeHeader);
    }

    default void writeCSV(File file, char separator, boolean writeHeader, boolean writeMetaFile) {
        DataFrameWriter.writeCSV(file, this, separator, writeHeader, writeMetaFile);
    }

    default void writeCSV(Writer writer, char separator, boolean writeHeader) {
        DataFrameWriter.writeCSV(writer, this, separator, writeHeader);
    }

    default void writeCSV(OutputStream outputStream, char separator, boolean writeHeader) {
        DataFrameWriter.writeCSV(outputStream, this, separator, writeHeader);

    }

    default void writeCSV(File file, char separator, String headerPrefix) {
        DataFrameWriter.writeCSV(file, this, separator, headerPrefix);
    }

    default void writeCSV(File file, char separator, String headerPrefix, boolean writeMetaFile) {
        DataFrameWriter.writeCSV(file, this, separator, headerPrefix, writeMetaFile);
    }

    default void writeCSV(Writer writer, char separator, String headerPrefix) {
        DataFrameWriter.writeCSV(writer, this, separator, headerPrefix);

    }

    default void writeCSV(OutputStream outputStream, char separator, String headerPrefix) {
        DataFrameWriter.writeCSV(outputStream, this, separator, headerPrefix);
    }

    default void print() {
        DataFrameWriter.print(this);
    }

    default void print(DataWriter dataWriter) {
        DataFrameWriter.print(this, dataWriter);

    }

    default void print(WriteFormat writeFormat) {
        DataFrameWriter.print(this, writeFormat);
    }
}
