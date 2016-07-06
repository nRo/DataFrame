package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.frame.column.*;
import de.unknownreality.dataframe.frame.filter.FilterPredicate;
import de.unknownreality.dataframe.frame.group.DataFrameGroupUtil;
import de.unknownreality.dataframe.frame.group.DataGrouping;
import de.unknownreality.dataframe.frame.index.Indices;
import de.unknownreality.dataframe.frame.join.DataFrameJoinUtil;
import de.unknownreality.dataframe.frame.join.JoinColumn;
import de.unknownreality.dataframe.frame.join.JoinedDataFrame;
import de.unknownreality.dataframe.frame.sort.RowColumnComparator;
import de.unknownreality.dataframe.frame.sort.SortColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrame implements DataContainer<DataFrameHeader, DataRow> {
    private static Logger log = LoggerFactory.getLogger(DataFrame.class);
    public static final String PRIMARY_INDEX_NAME = "primaryKey";
    private int size;
    private Map<String, DataFrameColumn> columnsMap = new LinkedHashMap<>();
    private LinkedHashSet<DataFrameColumn> columnList = new LinkedHashSet<>();
    private DataFrameHeader header = new DataFrameHeader();
    private Indices indices = new Indices(this);
    public DataFrame() {

    }

    public DataFrame(DataFrameHeader header, Collection<DataRow> rows) {
        set(header, rows);
    }

    public DataFrame setPrimaryKeyColumn(String... colNames) {
        return addIndex(PRIMARY_INDEX_NAME,colNames);
    }

    public DataFrame setPrimaryKeyColumn(DataFrameColumn... cols) {
        return addIndex(PRIMARY_INDEX_NAME,cols);
    }

    public DataFrame removePrimaryKey() {
        indices.removeIndex(PRIMARY_INDEX_NAME);
        return this;
    }

    public DataFrame removeIndex(String name){
        indices.removeIndex(name);
        return this;
    }

    public DataFrame renameColumn(String name, String newName) {
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

    public DataFrame addColumn(String name, DataFrameColumn column) {

        if (!columnList.isEmpty()) {
            if (column.size() != size) {
                throw new IllegalArgumentException(String.format("column lengths must be equal"));
            }
        }
        columnList.add(column);
        if(column.getDataFrame() != null && column.getDataFrame() != this){
            throw new IllegalArgumentException(String.format("column can not be added to multiple dataframes. use column.copy() first"));
        }
        if(columnList.size() == 1){
            this.size = column.size();
        }
        column.setDataFrame(this);
        header.add(name, column.getClass(), column.getType());
        columnsMap.put(name, column);
        return this;
    }

    public DataFrame addColumn(DataFrameColumn column) {
        return addColumn(column.getName(),column);
    }



    public <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name){
        return addColumn(type,name,ColumnConverter.create());
    }


    public <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name, ColumnConverter columnConverter){
        return addColumn(type,name,columnConverter,null);
    }

    public <T extends Comparable<T>> DataFrame addColumn(Class<T> type, String name, ColumnConverter columnConverter, ColumnAppender<T> appender) {
        Class<? extends DataFrameColumn<T>> columnType = columnConverter.getColumnType(type);
        if(columnType == null){
            throw new IllegalArgumentException(String.format("no  column type found for %s",type.getName()));
        }

        return addColumn(columnType,name,appender);
    }

    public <I extends Comparable<I>, T extends DataFrameColumn<I>> DataFrame addColumn(Class<T> cl, String name, ColumnAppender<I> appender) {
        try {
            T col = cl.newInstance();
            col.setName(name);
            if(appender != null){
                for (DataRow row : this) {
                    I val = appender.createRowValue(row);
                    if (val == null || val == Values.NA) {
                        col.appendNA();
                    } else {
                        col.append(val);
                    }
                }
            }
            addColumn(col);
        } catch (InstantiationException e) {
            log.error("error creating instance of column [{}], empty constructor required", cl, e);
        } catch (IllegalAccessException e) {
            log.error("error creating instance of column [{}], empty constructor required", cl, e);
        }
        ;
        return this;
    }

    public DataFrame addColumns(Collection<DataFrameColumn> columns) {
        for (DataFrameColumn column : columns) {
            addColumn(column);
        }
        return this;
    }

    public DataFrame addColumns(DataFrameColumn... columns) {
        for (DataFrameColumn column : columns) {
            addColumn(column);
        }
        return this;
    }

    public void append(Comparable... values) {
        if (values.length != columnList.size()) {
            throw new IllegalArgumentException(String.format("value for each column required"));
        }
        int i = 0;
        for (DataFrameColumn column : columnList) {
            if (values[i] != null && !column.getType().isInstance(values[i])) {
                throw new IllegalArgumentException(
                        String.format("value %i has wrong type (%s != %s)", i,
                                values[i].getClass().getName(),
                                column.getType().getName()));
            }
            i++;
        }
        i = 0;
        for (DataFrameColumn column : columnList) {
            column.startDataFrameAppend();
            Comparable value = values[i];
            if (value == null) {
                column.appendNA();
            } else {
                column.append(value);
            }
            column.endDataFrameAppend();
            i++;
        }
        size++;
        indices.update(getRow(size-1));
    }

    public void append(DataRow row) {
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
        indices.update(getRow(size-1));
    }

    public void set(Collection<DataRow> rows) {
        this.size = 0;
        this.indices.clearValues();
        for (DataFrameColumn column : columnsMap.values()) {
            column.clear();
        }
        for (DataRow row : rows) {
            append(row);
        }
    }

    private void set(DataFrameHeader header, Collection<DataRow> rows,Indices indices) {
        this.header = header;
        this.columnsMap.clear();
        this.columnList.clear();
        for (String h : header) {
            try {
                DataFrameColumn instance = header.getColumnType(h).newInstance();
                columnsMap.put(h, instance);
                columnList.add(instance);

            } catch (InstantiationException e) {
                log.error("error creating column instance", e);
            } catch (IllegalAccessException e) {
                log.error("error creating column instance", e);
            }
        }
        indices.copyTo(this);
        set(rows);
    }

    public void set(DataFrameHeader header, Collection<DataRow> rows) {
        this.header = header;
        this.columnsMap.clear();
        this.columnList.clear();
        this.indices.clearValues();
        for (String h : header) {
            try {
                DataFrameColumn instance = header.getColumnType(h).newInstance();
                instance.setName(h);
                addColumn(instance);
            } catch (InstantiationException e) {
                log.error("error creating column instance", e);
            } catch (IllegalAccessException e) {
                log.error("error creating column instance", e);
            }
        }
        set(rows);
    }
    public DataFrame removeColumn(String header) {
        DataFrameColumn column = getColumn(header);
        if (column == null) {
            log.error("error column not found '" + header + "'");
            return this;
        }
        return removeColumn(column);
    }
    public DataFrame removeColumn( DataFrameColumn column) {
        column.setDataFrame(null);
        this.header.remove(column.getName());
        this.indices.removeColumn(column);
        this.columnsMap.remove(header);
        this.columnList.remove(column);
        return this;
    }

    public void setHeader(DataFrameHeader header) {
        this.header = header;
    }


    public DataFrame sort(SortColumn... columns) {
        List<DataRow> rows = getRows(0, size);
        Collections.sort(rows, new RowColumnComparator(header, columns));
        set(rows);
        return this;
    }

    public DataFrame sort(Comparator<DataRow> comp) {
        List<DataRow> rows = getRows(0, size);
        Collections.sort(rows, comp);
        set(rows);
        return this;
    }

    public DataFrame sort(String name) {
        return sort(name, SortColumn.Direction.Ascending);
    }

    public DataFrame sort(String name, SortColumn.Direction dir) {
        List<DataRow> rows = getRows(0, size);
        Collections.sort(rows, new RowColumnComparator(header, new SortColumn[]{new SortColumn(name, dir)}));
        set(rows);
        return this;
    }

    public DataFrame find(String colName, Comparable value) {
        return find(FilterPredicate.eq(colName, value));
    }

    public DataRow findFirst(String colName, Comparable value) {
        return findFirst(FilterPredicate.eq(colName, value));
    }

    public DataRow findFirst(FilterPredicate predicate) {
        for (DataRow row : this) {
            if (predicate.valid(row)) {
                return row;
            }
        }
        return null;
    }

    public DataRow findByPrimaryKey(Comparable key) {
       return findByIndex(PRIMARY_INDEX_NAME,key);
    }


    public DataFrame filter(FilterPredicate predicate) {
        set(findRows(predicate));
        return this;
    }

    public DataFrame find(FilterPredicate predicate) {
        List<DataRow> rows = findRows(predicate);
        DataFrame df = new DataFrame();
        df.set(header.copy(), rows, indices);
        return df;
    }

    public List<DataRow> findRows(FilterPredicate predicate) {
        List<DataRow> rows = new ArrayList<>();
        for (DataRow row : this) {
            if (predicate.valid(row)) {
                rows.add(row);
            }
        }
        return rows;
    }


    public DataFrame reverse() {
        for (DataFrameColumn col : columnList) {
            col.reverse();
        }
        return this;
    }

    public DataFrame addIndex(String indexName,String... columnNames){
        DataFrameColumn[] columns = new DataFrameColumn[columnNames.length];
        for(int i  = 0; i < columns.length;i++){
            columns[i] = getColumn(columnNames[i]);
        }

        return addIndex(indexName,columns);
    }

    public DataFrame addIndex(String indexName,DataFrameColumn... columns){
        indices.addIndex(indexName, columns);
        return this;
    }


    public int size() {
        return size;
    }

    public DataFrame subset(int from, int to) {
        set(getRows(from, to));
        return this;
    }

    public DataFrame createSubset(int from, int to) {
        DataFrame newFrame = new DataFrame();
        newFrame.set(header.copy(), getRows(from, to));
        return newFrame;
    }

    public List<DataRow> getRows(int from, int to) {
        List<DataRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(getRow(i));
        }
        return rows;
    }


    public List<DataRow> getRows() {
        return getRows(0, size);
    }

    public DataFrameHeader getHeader() {
        return header;
    }

    public DataFrame concat(DataFrame frame) {
        if (!header.equals(frame.getHeader())) {
            throw new IllegalArgumentException(String.format("dataframes not compatible"));
        }
        for (DataRow row : frame) {
            append(row);
        }
        return this;
    }

    public DataFrame concat(Collection<DataFrame> dataFrames) {
        for (DataFrame dataFrame : dataFrames) {
            if (!header.equals(dataFrame.getHeader())) {
                throw new IllegalArgumentException(String.format("dataframes not compatible"));
            }
            for (DataRow row : dataFrame) {
                append(row);
            }
        }
        return this;
    }

    public DataFrame concat(DataFrame... dataFrames) {
        return concat(Arrays.asList(dataFrames));
    }

    public boolean isCompatible(DataFrame frame) {
        return header.equals(frame.getHeader());
    }


    public DataRow getRow(int i) {
        return new DataRow(header, getRowValues(i), i);
    }

    public Comparable[] getRowValues(int i){
        if (i >= size) {
            throw new IllegalArgumentException(String.format("index out of bounds"));
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

    public Collection<String> getColumnNames() {
        return new ArrayList<>(columnsMap.keySet());
    }

    public <T extends Comparable<T>> DataFrameColumn<T> getColumn(String name) {
        return columnsMap.get(name);
    }

    public <T extends DataFrameColumn> T getColumn(String name, Class<T> cl) {
        DataFrameColumn column = columnsMap.get(name);
        if (column == null) {
            throw new IllegalArgumentException(String.format("column '%s' not found", name));
        }
        if (!cl.isInstance(column)) {
            throw new IllegalArgumentException(String.format("column '%s' has wrong type", name));
        }
        return cl.cast(column);
    }

    public StringColumn getStringColumn(String name) {
        return getColumn(name, StringColumn.class);
    }

    public DoubleColumn getDoubleColumn(String name) {
        return getColumn(name, DoubleColumn.class);
    }

    public IntegerColumn getIntegerColumn(String name) {
        return getColumn(name, IntegerColumn.class);
    }

    public FloatColumn getFloatColumn(String name) {
        return getColumn(name, FloatColumn.class);
    }

    public BooleanColumn getBooleanColumn(String name) {
        return getColumn(name, BooleanColumn.class);
    }

    public DateColumn getDateColumn(String name) {
        return getColumn(name, DateColumn.class);
    }

    public DataGrouping groupBy(String... column) {
        return DataFrameGroupUtil.groupBy(this, column);
    }

    public JoinedDataFrame joinLeft(DataFrame dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinLeft(dataFrame, joinColumnsArray);
    }

    public JoinedDataFrame joinLeft(DataFrame dataFrame, JoinColumn... joinColumns) {
        return DataFrameJoinUtil.leftJoin(this, dataFrame, joinColumns);
    }

    public JoinedDataFrame joinLeft(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return DataFrameJoinUtil.leftJoin(this, dataFrame, suffixA, suffixB, joinColumns);
    }

    public JoinedDataFrame joinRight(DataFrame dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinRight(dataFrame, joinColumnsArray);
    }

    public JoinedDataFrame joinRight(DataFrame dataFrame, JoinColumn... joinColumns) {
        return DataFrameJoinUtil.rightJoin(this, dataFrame, joinColumns);
    }

    public JoinedDataFrame joinRight(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return DataFrameJoinUtil.rightJoin(this, dataFrame, suffixA, suffixB, joinColumns);
    }

    public JoinedDataFrame joinInner(DataFrame dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinInner(dataFrame, joinColumnsArray);
    }

    public JoinedDataFrame joinInner(DataFrame dataFrame, JoinColumn... joinColumns) {
        return DataFrameJoinUtil.innerJoin(this, dataFrame, joinColumns);
    }

    public JoinedDataFrame joinInner(DataFrame dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return DataFrameJoinUtil.innerJoin(this, dataFrame, suffixA, suffixB, joinColumns);
    }

    public DataFrame copy() {
        List<DataRow> rows = getRows(0, size);
        DataFrame copy = new DataFrame();
        copy.set(header.copy(), rows,indices);
        return copy;
    }

    public boolean containsColumn(DataFrameColumn column){
        return this.columnList.contains(column);
    }


    protected void notifyColumnValueChanged(DataFrameColumn column, int index, Comparable value){
        if(indices.isIndexColumn(column)){
            indices.updateValue(column,getRow(index));
        }
    }
    protected void notifyColumnChanged(DataFrameColumn column){
        if(indices.isIndexColumn(column)){
            indices.updateColumn(column);
        }
    }

    public boolean isIndexColumn(DataFrameColumn column){
        return indices.isIndexColumn(column);
    }

    public DataRow findByIndex(String name, Comparable... values){
        int i = indices.find(name,values);
        if(i >= 0){
            return getRow(i);
        }
        return null;
    }

    public Collection<DataFrameColumn> getColumns() {
        return columnList;
    }

    protected Indices getIndices() {
        return indices;
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            int index = 0;
            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public DataRow next() {
                return getRow(index++);
            }

            @Override
            public void remove() {

            }
        };
    }
}
