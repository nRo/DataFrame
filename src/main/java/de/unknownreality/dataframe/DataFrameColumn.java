package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.parser.Parser;

import java.util.Collection;
import java.util.List;

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


    /**
     * Sets a value at a specified index
     * @param index index of the new value
     * @param value value to be set
     * @return
     */
    public abstract DataFrameColumn<T> set(int index, T value);

    /**
     * {@linkplain MapFunction A map function} is applied to the values in this column.
     * {@link MapFunction<T>#map(T)} is called for each value.
     * @param mapFunction The function applied to each value
     * @return
     */
    public abstract DataFrameColumn<T> map(MapFunction<T> mapFunction);


    /**
     * Reverses the order of the column values
     */
    public abstract void reverse();


    /**
     * Returns the value at a specified index
     * @param index Index of the returned value
     * @return Value at the specified index
     */
    public abstract T get(int index);

    /**
     * Creates a copy of this column
     * @return The copy of this column
     */
    public abstract DataFrameColumn<T> copy();


    /**
     * Clears this column.
     * All values are removed and the size is set to 0
     */
    public abstract void clear();


    /**
     * Copies the values of this column to a specified array of the same type.
     * The size of the array and the column size must be equal.
     * @param a The array the values are copied to
     * @param <T>  Type of the values in this column
     * @return the array with the copied values.
     */
    public abstract <T> T[] toArray(T[] a);

    /**
     * Returns an {@link Object Object} array that contains all values of this column.
     * @return Object array with all column values
     */
    public abstract Object[] toArray();

    /**
     * Returns <tt>true</tt> if <b>all</b> values of a specified collection are present in this column.
     * @param c Collection that contains the tested values
     * @return <tt>true</tt> if all values are present in this column
     */
    public abstract boolean containsAll(Collection<?> c);

    /**
     * Returns <tt>true</tt> if the specified value exists in this column.
     * @param o value that is tested
     * @return <tt>true</tt> if this column contains the specified value
     */
    public abstract boolean contains(Object o);

    /**
     * A new value is appended at the end of this column.
     * @param value value to be appended
     * @return <tt>true</tt> if the value is successfully appended
     */
    public abstract boolean append(T value);

    /**
     * Appends a {@link Values#NA NA value} to the end of this column.
     */
    public abstract void appendNA();

    /**
     * Returns the number of values in this column.
     * @return Number of values in this column
     */
    public abstract int size();

    /**
     * Returns <tt>true</tt> if this column contains no values.
     * @return <tt>true</tt> if this column is empty.
     */
    public abstract boolean isEmpty();


    /**
     * Appends all values of in a collection to this column.
     * @param c collection containing values to be added to this column
     * @return <tt>true</tt> if all values are appended successfully
     */
    public abstract boolean appendAll(Collection<? extends T> c);


    /**
     * Returns <tt>true</tt> if the value at the specified index equals {@link Values#NA NA}.
     * @param index index to be tested for <tt>NA</tt>
     * @return <tt>true</tt> if the specified index is <tt>NA</tt>
     */
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
