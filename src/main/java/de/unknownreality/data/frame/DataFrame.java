package de.unknownreality.data.frame;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.RowIterator;
import de.unknownreality.data.frame.column.*;
import de.unknownreality.data.frame.filter.FilterPredicate;
import de.unknownreality.data.frame.group.DataFrameGroupUtil;
import de.unknownreality.data.frame.group.DataGrouping;
import de.unknownreality.data.frame.join.DataFrameJoinUtil;
import de.unknownreality.data.frame.join.JoinColumn;
import de.unknownreality.data.frame.join.JoinedDataFrame;
import de.unknownreality.data.frame.sort.RowColumnComparator;
import de.unknownreality.data.frame.sort.SortColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrame implements DataContainer<DataFrameHeader,DataRow>{
    private static Logger log = LoggerFactory.getLogger(DataFrame.class);
    private int size;
    private Map<String,DataColumn> columns = new LinkedHashMap<>();
    private DataFrameHeader header = new DataFrameHeader();


    public DataFrame(){

    }

    public DataFrame addColumn(String name, DataColumn column){
        if(columns.isEmpty()){
            this.size = column.size();
        }
        else{
            if(column.size() != size){
                throw new IllegalArgumentException(String.format("column lengths have to be equal"));
            }
        }
        header.add(name,column.getClass(),column.getType());
        columns.put(name,column);
        return this;
    }

    public DataFrame addColumn(DataColumn column){
        if(columns.isEmpty()){
            this.size = column.size();
        }
        else{
            if(column.size() != size){
                throw new IllegalArgumentException(String.format("column lengths have to be equal"));
            }
        }
        header.add(column);
        columns.put(column.getName(),column);
        return this;
    }
    public DataFrame addColumns(Collection<DataColumn> columns){
        for(DataColumn column : columns){
            addColumn(column);
        }
        return this;
    }
    public DataFrame addColumns(DataColumn... columns){
        for(DataColumn column : columns){
            addColumn(column);
        }
        return this;
    }

    public void append(Comparable... values){
        if(values.length != columns.size()){
            throw new IllegalArgumentException(String.format("value for each column required"));
        }
        int i = 0;
        for(DataColumn column : columns.values()){
            if(values[i] != null && !column.getType().isInstance(values[i])){
                throw new IllegalArgumentException(
                        String.format("value %i has wrong type (%s != %s)",i,
                                values[i].getClass().getName(),
                                column.getType().getName()));
            }
            i++;
        }
        i = 0;
        for(DataColumn column : columns.values()){
           if(values[i] == null){
               column.appendNA();
           }
            else{
               column.append(values[i]);
           }
            i++;
        }
        size++;
    }

    public void append(DataRow row){
        for(String h : header){
            DataColumn column = columns.get(h);
            if(row.isNA(h)){
                column.appendNA();
            }
            else{
                column.append(row.get(h));
            }
        }
        this.size++;
    }

    public void set(Collection<DataRow> rows){
        this.size = 0;
        for(DataColumn column : columns.values()){
            column.clear();
        }
        for(DataRow row : rows){
            append(row);
        }
    }

    public void set(DataFrameHeader header,Collection<DataRow> rows){
        this.header = header;
        this.columns.clear();
        for(String h : header){
            try {
                columns.put(h,header.getColumnType(h).newInstance());
            } catch (InstantiationException e) {
                log.error("error creating column instance",e);
            } catch (IllegalAccessException e) {
                log.error("error creating column instance",e);
            }
        }
        set(rows);
    }

    public void setHeader(DataFrameHeader header) {
        this.header = header;
    }

    public <I extends Comparable<I>,T extends DataColumn<I>> DataFrame addColumn(Class<T> cl, String name, ColumnAppender<I> appender){
        try {
            T col =  cl.newInstance();
            col.setName(name);
            for(DataRow row : this){
                I val = appender.createRowValue(row);
                if(val == null || val == Values.NA){
                    col.appendNA();
                }
                else{
                    col.append(val);
                }
            }
            addColumn(col);
        } catch (InstantiationException e) {
            log.error("error creating instance of column [{}], empty Constructor required",cl,e);
        } catch (IllegalAccessException e) {
            log.error("error creating instance of column [{}], empty Constructor required",cl,e);
        };
        return this;
    }

    public DataFrame sort(SortColumn... columns){
        List<DataRow> rows = getRows(0,size);
        rows.sort(new RowColumnComparator(header,columns));
        set(header,rows);
        return this;
    }
    public DataFrame sort(String name, SortColumn.Direction dir){
        List<DataRow> rows = getRows(0,size);
        rows.sort(new RowColumnComparator(header,new SortColumn[]{new SortColumn(name,dir)}));
        set(header,rows);
        return this;
    }

    public List<DataRow> find(String colName,Comparable value){
        return find(FilterPredicate.eq(colName,value));
    }

    public DataRow findFirst(String colName,Comparable value){
        return findFirst(FilterPredicate.eq(colName,value));

    }

    public DataRow findFirst(FilterPredicate predicate){
        for(DataRow row : this){
            if(predicate.valid(row)){
                return row;
            }
        }
        return null;
    }

    public DataFrame filter(FilterPredicate predicate){
        set(header,find(predicate));
        return this;
    }

    public List<DataRow> find(FilterPredicate predicate){
        List<DataRow> rows = new ArrayList<>();
        for(DataRow row : this){
            if(predicate.valid(row)){
                rows.add(row);
            }
        }
        return rows;
    }

    public DataFrame reverse(){
        for(DataColumn col : columns.values()){
            col.reverse();
        }
        return this;
    }


    public int getSize() {
        return size;
    }

    public DataFrame subset(int from, int to){
        set(header,getRows(from,to));
        return this;
    }

    public DataFrame createSubset(int from, int to){
        DataFrame newFrame = new DataFrame();
        newFrame.set(header.copy(),getRows(from,to));
        return newFrame;
    }

    public List<DataRow> getRows(int from, int to){
        List<DataRow> rows = new ArrayList<>();
        for(int i = from; i < to; i++){
            rows.add(getRow(i));
        }
        return rows;
    }


    public List<DataRow> getRows(){
        return getRows(0,size);
    }

    public DataFrameHeader getHeader() {
        return header;
    }

    public DataFrame concat(DataFrame frame){
        if(!header.equals(frame.getHeader())){
            throw new IllegalArgumentException(String.format("dataframes not compatible"));
        }
        for(DataRow row : frame){
            append(row);
        }
        return this;
    }
    public DataFrame concat(Collection<DataFrame> dataFrames){
        for(DataFrame dataFrame : dataFrames){
            if(!header.equals(dataFrame.getHeader())){
                throw new IllegalArgumentException(String.format("dataframes not compatible"));
            }
            for(DataRow row : dataFrame){
                append(row);
            }
        }
        return this;
    }
    public DataFrame concat(DataFrame... dataFrames){
        return concat(Arrays.asList(dataFrames));
    }

    public boolean isCompatible(DataFrame frame){
        return header.equals(frame.getHeader());
    }

    public void write(File file){
        write(file,"\t");
    }
    public void write(File file,String separator){
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))){
            writer.write("#");
            for(int i = 0; i < header.size();i++){
                writer.write(header.get(i));
                if(i < header.size() - 1){
                    writer.write(separator);
                }
            }
            writer.newLine();
            for(DataRow row : this){
                for(int i = 0; i < row.size();i++){
                    writer.write(row.get(i).toString());
                    if(i < row.size() - 1){
                        writer.write(separator);
                    }
                }
                writer.newLine();
            }
        } catch (IOException e) {
            log.error("error writing {}",file,e);
        }
    }


    public DataRow getRow(int i){
        if(i >= size){
            throw new IllegalArgumentException(String.format("index out of bounds"));
        }
        Comparable[] values = new Comparable[columns.size()];
        int j = 0;
        for(DataColumn column : columns.values()){
            if(column.isNA(i)){
                values[j++] = Values.NA;
            }
            else{
                values[j++] = column.get(i);
            }
        }
        return new DataRow(header,values,i);
    }

    public Collection<String> getColumnNames(){
        return new ArrayList<>(columns.keySet());
    }

    public <T extends Comparable<T>> DataColumn<T> getColumn(String name){
        return columns.get(name);
    }

    public <T extends DataColumn> T getColumn(String name,Class<T> cl){
        DataColumn column = columns.get(name);
        if(column == null){
            throw new IllegalArgumentException(String.format("column '%s' not found",name));
        }
        if(!cl.isInstance(column)){
            throw new IllegalArgumentException(String.format("column '%s' has wrong type",name));
        }
        return cl.cast(column);
    }

    public StringColumn getStringColumn(String name){
        return getColumn(name,StringColumn.class);
    }

    public DoubleColumn getDoubleColumn(String name){
        return getColumn(name,DoubleColumn.class);
    }

    public IntegerColumn getIntegerColumn(String name){
        return getColumn(name,IntegerColumn.class);
    }
    public FloatColumn getFloatColumn(String name){
        return getColumn(name,FloatColumn.class);
    }
    public BooleanColumn getBooleanColumn(String name){
        return getColumn(name,BooleanColumn.class);
    }
    public DateColumn getDateColumn(String name){
        return getColumn(name,DateColumn.class);
    }

    public DataGrouping groupBy(String... column){
        return DataFrameGroupUtil.groupBy(this,column);
    }

    public JoinedDataFrame joinLeft(DataFrame dataFrame,String... joinColumns){
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for(int i = 0; i < joinColumns.length;i++){
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinLeft(dataFrame,joinColumnsArray);
    }

    public JoinedDataFrame joinLeft(DataFrame dataFrame, JoinColumn... joinColumns){
        return DataFrameJoinUtil.leftJoin(this,dataFrame,joinColumns);
    }

    public JoinedDataFrame joinLeft(DataFrame dataFrame,String suffixA,String suffixB,JoinColumn... joinColumns){
        return DataFrameJoinUtil.leftJoin(this,dataFrame,suffixA,suffixB,joinColumns);
    }

    public JoinedDataFrame joinRight(DataFrame dataFrame,String... joinColumns){
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for(int i = 0; i < joinColumns.length;i++){
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinRight(dataFrame,joinColumnsArray);
    }

    public JoinedDataFrame joinRight(DataFrame dataFrame, JoinColumn... joinColumns){
        return DataFrameJoinUtil.rightJoin(this,dataFrame,joinColumns);
    }

    public JoinedDataFrame joinRight(DataFrame dataFrame,String suffixA,String suffixB,JoinColumn... joinColumns){
        return DataFrameJoinUtil.rightJoin(this,dataFrame,suffixA,suffixB,joinColumns);
    }

    public JoinedDataFrame joinInner(DataFrame dataFrame,String... joinColumns){
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for(int i = 0; i < joinColumns.length;i++){
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinInner(dataFrame,joinColumnsArray);
    }

    public JoinedDataFrame joinInner(DataFrame dataFrame, JoinColumn... joinColumns){
        return DataFrameJoinUtil.innerJoin(this,dataFrame,joinColumns);
    }

    public JoinedDataFrame joinInner(DataFrame dataFrame,String suffixA,String suffixB,JoinColumn... joinColumns){
        return DataFrameJoinUtil.innerJoin(this,dataFrame,suffixA,suffixB,joinColumns);
    }
    public DataFrame copy(){
        List<DataRow> rows = getRows(0,size);
        DataFrame copy = new DataFrame();
        copy.set(header.copy(),rows);
        return copy;
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
        };
    }
}
