/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.*;
import de.unknownreality.dataframe.common.mapping.DataMapper;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.filter.compile.PredicateCompiler;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupUtil;
import de.unknownreality.dataframe.group.impl.DefaultGroupUtil;
import de.unknownreality.dataframe.index.Index;
import de.unknownreality.dataframe.index.Indices;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinUtil;
import de.unknownreality.dataframe.join.JoinedDataFrame;
import de.unknownreality.dataframe.join.impl.DefaultJoinUtil;
import de.unknownreality.dataframe.sort.RowColumnComparator;
import de.unknownreality.dataframe.sort.SortColumn;
import de.unknownreality.dataframe.transform.DataFrameTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class DefaultDataFrame implements DataFrame {
    private static final Logger log = LoggerFactory.getLogger(DefaultDataFrame.class);
    private int size;
    private final Map<String, DataFrameColumn> columnsMap = new LinkedHashMap<>();
    private final List<DataFrameColumn> columnList = new ArrayList<>();
    private DataFrameHeader header = new DataFrameHeader();
    private final Indices indices = new Indices(this);
    private JoinUtil joinUtil = new DefaultJoinUtil();
    private GroupUtil groupUtil = new DefaultGroupUtil();

    public DefaultDataFrame() {

    }

    /**
     * Creates a new data frame using a data frame header and a collections of data rows
     *
     * @param header data frame header
     * @param rows   collections of data rows
     */
    public DefaultDataFrame(DataFrameHeader header, Collection<DataRow> rows) {
        set(header, rows);
    }



    @Override
    public DefaultDataFrame setPrimaryKey(String... colNames) {
        DataFrameColumn[] columns = new DataFrameColumn[colNames.length];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = getColumn(colNames[i]);
        }
        return setPrimaryKey(columns);
    }


    @Override
    public DefaultDataFrame setPrimaryKey(DataFrameColumn... cols) {
        this.indices.setPrimaryKey(cols);
        return this;
    }


    @Override
    public DefaultDataFrame removePrimaryKey() {
        indices.removeIndex(Indices.PRIMARY_KEY_NAME);
        return this;
    }


    @Override
    public DefaultDataFrame removeIndex(String name) {
        indices.removeIndex(name);
        return this;
    }


    @Override
    public DefaultDataFrame renameColumn(String name, String newName) {
        DataFrameColumn column = columnsMap.get(name);
        if (column == null) {
            return this;
        }
        header.rename(name, newName);
        column.setName(newName);
        columnsMap.remove(name);
        columnsMap.put(newName, column);
        return this;
    }


    public DefaultDataFrame replaceColumn(String existing, DataFrameColumn replacement) {
        DataFrameColumn existingColumn = getColumn(existing);
        return replaceColumn(existingColumn, replacement);
    }


    public DefaultDataFrame replaceColumn(DataFrameColumn existing, DataFrameColumn replacement) {
        int existingIndex = header.getIndex(existing.getName());
        columnList.set(existingIndex, replacement);
        header.replace(existing, replacement);
        columnsMap.remove(existing.getName());
        columnsMap.put(replacement.getName(), replacement);
        indices.replace(existing, replacement);
        return this;
    }


    @Override
    @SuppressWarnings("unchecked")
    public DefaultDataFrame addColumn(DataFrameColumn column) {
        if (!columnList.isEmpty() && column.size() != size) {
            throw new DataFrameRuntimeException("column lengths must be equal");
        }
        columnList.add(column);
        if (column.getDataFrame() != null && column.getDataFrame() != this) {
            throw new DataFrameRuntimeException("column can not be added to multiple data frames. use column.copy() first");
        }
        if (columnList.size() == 1) {
            this.size = column.size();
        }
        try {
            column.setDataFrame(this);
        } catch (DataFrameException e) {
            throw new DataFrameRuntimeException("error adding column", e);
        }
        header.add(column.getName(), column.getClass(), column.getType());
        columnsMap.put(column.getName(), column);
        return this;
    }



    public DefaultDataFrame addBooleanColumn(String name) {
        BooleanColumn column = new BooleanColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addByteColumn(String name) {
        ByteColumn column = new ByteColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addDoubleColumn(String name) {
        DoubleColumn column = new DoubleColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addFloatColumn(String name) {
        FloatColumn column = new FloatColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addIntegerColumn(String name) {
        IntegerColumn column = new IntegerColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addLongColumn(String name) {
        LongColumn column = new LongColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addShortColumn(String name) {
        ShortColumn column = new ShortColumn(name);
        return addColumn(column);
    }


    public DefaultDataFrame addStringColumn(String name) {
        StringColumn column = new StringColumn(name);
        return addColumn(column);
    }



    @Override
    public <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name) {
        return addColumn(type, name, ColumnTypeMap.create());
    }



    @Override
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap) {
        return addColumn(type, name, columnTypeMap, null);
    }


    @Override
    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame addColumn(Class<T> type, String name, ColumnTypeMap columnTypeMap, ColumnAppender<T> appender) {
        Class<C> columnType = columnTypeMap.getColumnType(type);
        if (columnType == null) {
            throw new DataFrameRuntimeException(String.format("no  column type found for %s", type.getName()));
        }

        return addColumn(columnType, name, appender);
    }

    /**
     * {@inheritDoc}
     * If no column appender is specified, the column is filled with {@link Values#NA NA} values.
     * If the column can not be created or added a {@link DataFrameRuntimeException} is thrown.
     */
    @Override
    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrame addColumn(Class<C> type, String name, ColumnAppender<T> appender) {
        try {
            C col = type.newInstance();
            col.setName(name);
            if (appender != null) {
                for (DataRow row : this) {
                    T val = appender.createRowValue(row);
                    if (val == null || val == Values.NA) {
                        col.doAppendNA();
                    } else {
                        col.doAppend(val);
                    }
                }
            } else {
                for (int i = 0; i < size(); i++) {
                    col.doAppendNA();
                }
            }
            addColumn(col);
        } catch (InstantiationException e) {
            log.error("error creating instance of column [{}], empty constructor required", type, e);
            throw new DataFrameRuntimeException(String.format("error creating instance of column [%s], empty constructor required", type), e);

        } catch (IllegalAccessException e) {
            throw new DataFrameRuntimeException(String.format("error creating instance of column [%s], empty constructor required", type), e);
        }
        return this;
    }



    @Override
    public DefaultDataFrame addColumns(Collection<DataFrameColumn> columns) {
        for (DataFrameColumn column : columns) {
            addColumn(column);
        }
        return this;
    }


    @Override
    public DefaultDataFrame addColumns(DataFrameColumn... columns) {
        for (DataFrameColumn column : columns) {
            addColumn(column);
        }
        return this;
    }

    /**
     * {@inheritDoc} If the wrong number of values or a wrong type is found a {@link DataFrameRuntimeException} is thrown.
     */
    @Override
    public DefaultDataFrame append(Comparable... values) {
        if (values.length != columnList.size()) {
            throw new DataFrameRuntimeException("value for each column required");
        }
        int i = 0;
        for (DataFrameColumn column : columnList) {
            if (values[i] != null && values[i] != Values.NA && !column.getType().isAssignableFrom(values[i].getClass())) {
                throw new DataFrameRuntimeException(
                        String.format("value %d has wrong type (%s != %s)", i,
                                values[i].getClass().getName(),
                                column.getType().getName()));
            }
            i++;
        }
        i = 0;
        for (DataFrameColumn<?, ?> column : columnList) {
            column.startDataFrameAppend();
            Comparable<?> value = values[i];
            if (value == null || value == Values.NA) {
                column.appendNA();
            } else {
                column.append(value);
            }
            column.endDataFrameAppend();
            i++;
        }
        size++;
        indices.update(getRow(size - 1));
        return this;
    }

    /**
     * @param row row containing the new values
     * @return <tt>self</tt> for method chaining
     * {@inheritDoc} {@link Values#NA NA} is added for all columns with no value in the provided row.
     */
    @Override
    @SuppressWarnings("unchecked")
    public DefaultDataFrame append(DataRow row) {
        for (String h : header) {
            DataFrameColumn column = columnsMap.get(h);
            column.startDataFrameAppend();
            if (row.isNA(h)) {
                column.appendNA();
            } else {
                column.append(row.get(h));
            }
            column.endDataFrameAppend();

        }
        this.size++;
        indices.update(getRow(size - 1));
        return this;
    }


    @Override
    public DefaultDataFrame update(DataRow dataRow) {
        for (String h : header) {
            DataFrameColumn column = getColumn(h);
            Comparable newValue = dataRow.get(h);
            if (newValue == null) {
                continue;
            }
            if (newValue == Values.NA) {
                column.setNA(dataRow.getIndex());
            } else {
                column.set(dataRow.getIndex(), newValue);
            }
        }
        return this;
    }


    @Override
    public DefaultDataFrame set(Collection<DataRow> rows) {
        this.size = 0;
        this.indices.clearValues();
        for (DataFrameColumn column : columnsMap.values()) {
            column.clear();
        }
        for (DataRow row : rows) {
            append(row);
        }
        return this;
    }


    @Override
    public DefaultDataFrame set(DataFrameHeader header, Collection<DataRow> rows) {
        return set(header, rows, null);
    }


    protected DefaultDataFrame set(DataFrameHeader header, Collection<DataRow> rows, Indices indices) {
        this.header = header;
        this.columnsMap.clear();
        this.columnList.clear();
        this.indices.clearValues();
        for (String h : header) {
            try {
                DataFrameColumn instance = header.getColumnType(h).newInstance();
                instance.setName(h);
                columnsMap.put(h, instance);
                columnList.add(instance);
                instance.setDataFrame(this);

            } catch (InstantiationException | IllegalAccessException | DataFrameException e) {
                log.error("error creating column instance", e);
                throw new DataFrameRuntimeException("error creating column instance", e);

            }
        }
        if (indices != null) {
            indices.copyTo(this);
        }
        set(rows);
        return this;
    }



    @Override
    public DefaultDataFrame removeColumn(String header) {
        DataFrameColumn column = getColumn(header);
        if (column == null) {
            log.error("error column not found {}", header);
            return this;
        }
        return removeColumn(column);
    }


    @Override
    public DefaultDataFrame removeColumn(DataFrameColumn column) {
        try {
            column.setDataFrame(null);
        } catch (DataFrameException e) {
            throw new DataFrameRuntimeException("error removing column", e);

        }
        this.header.remove(column.getName());
        this.indices.removeColumn(column);
        this.columnsMap.remove(column.getName());
        this.columnList.remove(column);
        return this;
    }



    @Override
    public DefaultDataFrame sort(SortColumn... columns) {
        List<DataRow> rows = getRows(0, size);
        Collections.sort(rows, new RowColumnComparator(columns));
        set(rows);
        return this;
    }


    @Override
    public DefaultDataFrame sort(Comparator<DataRow> comp) {
        List<DataRow> rows = getRows(0, size);
        Collections.sort(rows, comp);
        set(rows);
        return this;
    }


    @Override
    public DefaultDataFrame sort(String name) {
        return sort(name, SortColumn.Direction.Ascending);
    }


    @Override
    public DefaultDataFrame sort(String name, SortColumn.Direction dir) {
        List<DataRow> rows = getRows(0, size);
        Collections.sort(rows, new RowColumnComparator(new SortColumn[]{new SortColumn(name, dir)}));
        set(rows);
        return this;
    }



    @Override
    public DefaultDataFrame shuffle() {
        List<DataRow> rows = getRows(0, size);
        Collections.shuffle(rows);
        set(rows);
        return this;
    }



    @Override
    public DefaultDataFrame select(String colName, Comparable value) {
        return select(FilterPredicate.eq(colName, value));
    }



    @Override
    public DataRow selectFirst(String colName, Comparable value) {
        return selectFirst(FilterPredicate.eq(colName, value));
    }



    @Override
    public DataRow selectFirst(String predicateString) {
        return selectFirst(FilterPredicate.compile(predicateString));
    }


    @Override
    public DataRow selectFirst(FilterPredicate predicate) {
        for (DataRow row : this) {
            if (predicate.valid(row)) {
                return row;
            }
        }
        return null;
    }



    @Override
    public DefaultDataFrame select(FilterPredicate predicate) {
        List<DataRow> rows = selectRows(predicate);
        DefaultDataFrame df = new DefaultDataFrame();
        df.set(header.copy(), rows, indices);
        return df;
    }



    @Override
    public DefaultDataFrame select(String predicateString) {
        return select(PredicateCompiler.compile(predicateString));
    }



    @Override
    public DefaultDataFrame filter(String predicateString) {
        filter(FilterPredicate.compile(predicateString));
        return this;
    }


    @Override
    public DefaultDataFrame filter(FilterPredicate predicate) {
        set(selectRows(predicate));
        return this;
    }



    @Override
    public List<DataRow> selectRows(String predicateString) {
        return selectRows(FilterPredicate.compile(predicateString));
    }


    @Override
    public List<DataRow> selectRows(FilterPredicate predicate) {
        List<DataRow> rows = new ArrayList<>();
        for (DataRow row : this) {
            if (predicate.valid(row)) {
                rows.add(row);
            }
        }
        return rows;
    }



    @Override
    public DefaultDataFrame transform(DataFrameTransform transformer) {
        return transformer.transform(this);
    }


    @Override
    public DataRow findByPrimaryKey(Comparable... keyValues) {
        Integer index = this.indices.findByPrimaryKey(keyValues);
        if (index == null || index < 0) {
            return null;
        }
        return getRow(index);
    }


    @Override
    public DefaultDataFrame reverse() {
        for (DataFrameColumn col : columnList) {
            col.doReverse();
        }
        return this;
    }


    @Override
    public DefaultDataFrame addIndex(String indexName, String... columnNames) {
        DataFrameColumn[] columns = new DataFrameColumn[columnNames.length];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = getColumn(columnNames[i]);
        }

        return addIndex(indexName, columns);
    }


    @Override
    public DefaultDataFrame addIndex(String indexName, DataFrameColumn... columns) {
        indices.addIndex(indexName, columns);
        return this;
    }

    @Override
    public DefaultDataFrame addIndex(Index index) {
        indices.addIndex(index);
        return this;
    }

    @Override
    public int size() {
        return size;
    }



    @Override
    public DefaultDataFrame subset(int from, int to) {
        set(getRows(from, to));
        return this;
    }


    @Override
    public DefaultDataFrame createSubset(int from, int to) {
        DefaultDataFrame newFrame = new DefaultDataFrame();
        newFrame.set(header.copy(), getRows(from, to), indices);
        return newFrame;
    }


    @Override
    public List<DataRow> getRows(int from, int to) {
        List<DataRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(getRow(i));
        }
        return rows;
    }


    @Override
    public List<DataRow> getRows() {
        return getRows(0, size);
    }


    @Override
    public DataFrameHeader getHeader() {
        return header;
    }



    @Override
    public DefaultDataFrame concat(DataFrame other) {
        if (!header.equals(other.getHeader())) {
            throw new DataFrameRuntimeException("data frames not compatible");
        }
        for (DataRow row : other) {
            append(row);
        }
        return this;
    }


    @Override
    public DefaultDataFrame concat(Collection<DataFrame> dataFrames) {
        for (DataFrame dataFrame : dataFrames) {
            if (!header.equals(dataFrame.getHeader())) {
                throw new DataFrameRuntimeException("data frames not compatible");
            }
            for (DataRow row : dataFrame) {
                append(row);
            }
        }
        return this;
    }


    @Override
    public DefaultDataFrame concat(DataFrame... dataFrames) {
        return concat(Arrays.asList(dataFrames));
    }


    @Override
    public boolean isCompatible(DataFrame input) {
        return header.equals(input.getHeader());
    }


    @Override
    public DataRow getRow(int i) {
        return new DataRow(header, getRowValues(i), i);
    }


    @Override
    public Comparable[] getRowValues(int i) {
        if (i >= size) {
            throw new DataFrameRuntimeException("index out of bounds");
        }
        Comparable[] values = new Comparable[columnList.size()];
        int j = 0;
        for (DataFrameColumn column : columnList) {
            if (column.isNA(i)) {
                values[j++] = Values.NA;
            } else {
                values[j++] = column.get(i);
            }
        }
        return values;
    }


    @Override
    public Collection<String> getColumnNames() {
        return new ArrayList<>(columnsMap.keySet());
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T extends Comparable<T>, C extends DataFrameColumn<T, C>> DataFrameColumn<T, C> getColumn(String name) {
        return columnsMap.get(name);
    }


    @Override
    public <T extends DataFrameColumn> T getColumn(String name, Class<T> cl) {
        DataFrameColumn column = columnsMap.get(name);
        if (column == null) {
            throw new DataFrameRuntimeException(String.format("column '%s' not found", name));
        }
        if (!cl.isInstance(column)) {
            throw new DataFrameRuntimeException(String.format("column '%s' has wrong type", name));
        }
        return cl.cast(column);
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T extends Number & Comparable<T>, C extends NumberColumn<T, C>> NumberColumn<T, C> getNumberColumn(String name) {
        return getColumn(name, NumberColumn.class);
    }


    @Override
    public StringColumn getStringColumn(String name) {
        return getColumn(name, StringColumn.class);
    }


    @Override
    public DoubleColumn getDoubleColumn(String name) {
        return getColumn(name, DoubleColumn.class);
    }


    @Override
    public IntegerColumn getIntegerColumn(String name) {
        return getColumn(name, IntegerColumn.class);
    }


    @Override
    public FloatColumn getFloatColumn(String name) {
        return getColumn(name, FloatColumn.class);
    }


    @Override
    public BooleanColumn getBooleanColumn(String name) {
        return getColumn(name, BooleanColumn.class);
    }


    @Override
    public ByteColumn getByteColumn(String name) {
        return getColumn(name, ByteColumn.class);
    }


    @Override
    public LongColumn getLongColumn(String name) {
        return getColumn(name, LongColumn.class);
    }


    @Override
    public ShortColumn getShortColumn(String name) {
        return getColumn(name, ShortColumn.class);
    }



    @Override
    public DataGrouping groupBy(String... column) {
        return groupUtil.groupBy(this, column);
    }


    @Override
    public JoinedDataFrame joinLeft(DataFrame dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinLeft(dataFrame, joinColumnsArray);
    }


    @Override
    public JoinedDataFrame joinLeft(DataFrame dataFrame, JoinColumn... joinColumns) {
        return joinUtil.leftJoin(this, dataFrame, joinColumns);
    }


    @Override
    public JoinedDataFrame joinLeft(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return joinUtil.leftJoin(this, dataFrame, suffixA, suffixB, joinColumns);
    }


    @Override
    public JoinedDataFrame joinRight(DataFrame dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinRight(dataFrame, joinColumnsArray);
    }


    @Override
    public JoinedDataFrame joinRight(DataFrame dataFrame, JoinColumn... joinColumns) {
        return joinUtil.rightJoin(this, dataFrame, joinColumns);
    }


    @Override
    public JoinedDataFrame joinRight(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return joinUtil.rightJoin(this, dataFrame, suffixA, suffixB, joinColumns);
    }


    @Override
    public JoinedDataFrame joinInner(DataFrame dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinInner(dataFrame, joinColumnsArray);
    }


    @Override
    public JoinedDataFrame joinInner(DataFrame dataFrame, JoinColumn... joinColumns) {
        return joinUtil.innerJoin(this, dataFrame, joinColumns);
    }


    @Override
    public JoinedDataFrame joinInner(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return joinUtil.innerJoin(this, dataFrame, suffixA, suffixB, joinColumns);
    }


    @Override
    public DefaultDataFrame copy() {
        List<DataRow> rows = getRows(0, size);
        DefaultDataFrame copy = new DefaultDataFrame();
        copy.set(header.copy(), rows, indices);
        return copy;
    }


    @Override
    public boolean containsColumn(DataFrameColumn column) {
        return this.columnList.contains(column);
    }


    protected void notifyColumnValueChanged(DataFrameColumn column, int index, Comparable value) {
        if (indices.isIndexColumn(column)) {
            indices.updateValue(column, getRow(index));
        }
    }


    protected void notifyColumnChanged(DataFrameColumn column) {
        if (indices.isIndexColumn(column)) {
            indices.updateColumn(column);
        }
    }


    @Override
    public boolean isIndexColumn(DataFrameColumn column) {
        return indices.isIndexColumn(column);
    }


    @Override
    public List<DataRow> findByIndex(String name, Comparable... values) {
        Collection<Integer> rowIndices = indices.find(name, values);
        if (!rowIndices.isEmpty()) {
            List<DataRow> rows = new ArrayList<>();
            for (Integer i : rowIndices) {
                rows.add(getRow(i));
            }
            return rows;
        }
        return new ArrayList<>(0);
    }


    @Override
    public DataRow findFirstByIndex(String name, Comparable... values) {
        Integer idx = indices.findFirst(name, values);
        return idx == null ? null : getRow(idx);
    }


    @Override
    public Collection<DataFrameColumn> getColumns() {
        return columnList;
    }

    @Override
    public Iterable<? extends DataRow> rows() {
        return this;
    }


    protected Indices getIndices() {
        return indices;
    }

    public GroupUtil getGroupUtil() {
        return groupUtil;
    }

    public JoinUtil getJoinUtil() {
        return joinUtil;
    }

    public void setGroupUtil(GroupUtil groupUtil) {
        this.groupUtil = groupUtil;
    }

    public void setJoinUtil(JoinUtil joinUtil) {
        this.joinUtil = joinUtil;
    }

    @Override
    public <T> List<T> map(Class<T> cl) {
        return DataMapper.map(this, cl);
    }


    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public DataRow next() {
                if (index == size()) {
                    throw new NoSuchElementException("index out of bounds");
                }
                return getRow(index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported for data frames");
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof DefaultDataFrame)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        DataFrame d = (DataFrame) o;
        if (size() != d.size()) {
            return false;
        }

        for (int i = 0; i < size(); i++) {
            if (!getRow(i).equals(d.getRow(i))) {
                return false;
            }
        }
        return true;
    }
}
