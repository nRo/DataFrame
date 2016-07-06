package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.common.parser.Parser;

import java.util.Collection;

/**
 * Created by Alex on 11.03.2016.
 */
public abstract class DataFrameColumn<T extends Comparable<T>> implements Iterable<T> {
    private String name;
    private DataFrame dataFrame;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract Parser<T> getParser();

    public abstract Class<T> getType();

    public abstract DataFrameColumn<T> set(int index, T value);

    public abstract DataFrameColumn<T> map(MapFunction<T> dataFunction);

    public abstract void reverse();

    public abstract T get(int index);

    public abstract DataFrameColumn<T> copy();

    public abstract void clear();

    public abstract <T1> T1[] toArray(T1[] a);

    public abstract Object[] toArray();

    public abstract boolean containsAll(Collection<?> c);

    public abstract boolean contains(Object o);

    public abstract boolean append(T value);

    public abstract int size();

    public abstract boolean isEmpty();
    public abstract boolean appendAll(Collection<T> c);

    public abstract void appendNA();

    public abstract boolean isNA(int index);
    public abstract  void setNA(int index);

    private boolean dataFrameAppend = false;

    public void validateAppend(){
        if(!dataFrameAppend && getDataFrame() != null){
            throw new IllegalArgumentException("append can only be used if the column is not added to a dataframe. use dataFrame.append()");
        }
    }
    public void notifyDataFrameValueChanged(int index){
        if(dataFrame == null){
            return;
        }
        dataFrame.notifyColumnValueChanged(this,index,get(index));
    }

    public void notifyDataFrameColumnChanged(){
        if(dataFrame == null){
            return;
        }
        dataFrame.notifyColumnChanged(this);
    }
    protected void startDataFrameAppend(){
        this.dataFrameAppend = true;
    }

    protected void endDataFrameAppend(){
        this.dataFrameAppend = false;
    }

    public DataFrame getDataFrame() {
        return dataFrame;
    }

    protected void setDataFrame(DataFrame dataFrame) {
        if(dataFrame == null){
            this.dataFrame = null;
            return;
        }
        if(!dataFrame.containsColumn(this)){
            throw new IllegalArgumentException("setDataFrame is only used internally. please use dataFrame.addColumn");
        }
        this.dataFrame = dataFrame;
    }
}
