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

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.transform.ColumnDataFrameTransform;
import de.unknownreality.dataframe.transform.ColumnTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;

/**
 * Created by Alex on 11.03.2016.
 */
public abstract class DataFrameColumn<T extends Comparable<T>, C extends DataFrameColumn<T, C>> implements Iterable<T> {
    private static final Logger log = LoggerFactory.getLogger(DataFrameColumn.class);
    public static final String ERROR_APPENDING = "error appending value to column";
    private String name;
    private DefaultDataFrame dataFrame;
    private boolean dataFrameAppend = false;


    /**
     * Used to return the right column type for
     *
     * @return <tt>self</tt>
     */
    protected abstract C getThis();

    /**
     * Sets the capacity of this column.
     * Can be used during dataframe creation if the size is known.
     * @param capacity capacity
     */
    public abstract C setCapacity(int capacity);

    /**
     * Used to apply transformations on a column
     * @param transformer column transformer
     * @param <R> type of resulting column
     * @return resulting column
     */
    public <R extends DataFrameColumn<?,R>> R transform(ColumnTransform<C,R> transformer){
        return transformer.transform(getThis());
    }

    /**
     * Used to apply transformations on a column
     * @param transformer column transformer
     * @return resulting column
     */
    public DataFrame transform(ColumnDataFrameTransform<C> transformer){
        return transformer.transform(getThis());
    }

    /**
     * Used by {@link #sort(Comparator)} to sort the values in this column
     *
     * @param comparator sort comparator
     * @see #sort()
     */
    protected abstract void doSort(Comparator<T> comparator);

    /**
     * Sorts values in the column using a provided comparator
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param comparator the comparator used to sort the column values
     * @return <tt>self</tt> for method chaining
     */
    public final C sort(Comparator<T> comparator) {
        doSort(comparator);
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Used by {@link #sort()} to sort the column values by their {@linkplain Comparable natural ordering}
     *
     * @see #sort()
     */
    protected abstract void doSort();

    /**
     * Sorts the column values by their {@linkplain Comparable natural ordering}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @return <tt>self</tt> for method chaining
     */
    public final C sort() {
        doSort();
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Returns the double value at a specified index.
     * If no double value is found, the value is parsed to double
     * If the value could not be parsed Double.NaN is returned
     * @param index index of value
     * @return double value
     */
    public Double toDouble(int index){
        Comparable v = get(index);
        try{
            return Number.class.cast(v).doubleValue();
        }
        catch (Exception e){
            // try parsing now
        }
        try{
            return Double.parseDouble(String.valueOf(v));
        }
        catch (Exception e){
            return Double.NaN;
        }
    }

    /**
     * Returns the name of this column
     *
     * @return name of this column
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this column.
     *
     * @param name new name of this column
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns a {@link Parser} for the type of the values in this column.
     *
     * @return parser for the value type in this column
     */
    public abstract Parser<T> getParser();

    /**
     * Returns the type of the values in this column.
     * The type needs to extend {@link Comparable}
     *
     * @return type of the values in this column
     */
    public abstract Class<T> getType();


    /**
     * Used by {@link #set(int, Comparable)} to set a value at a specified index
     *
     * @param index index of the new value
     * @param value value to be doSet
     * @see #set(int, Comparable)
     */
    protected abstract void doSet(int index, T value);

    /**
     * Sets a value at a specified index.
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param index index of the new value
     * @param value value to be doSet
     * @return <tt>self</tt> for method chaining
     */
    public final C set(int index, T value) {
        doSet(index, value);
        notifyDataFrameValueChanged(index);
        return getThis();
    }


    /**
     * Used by {@link #map(MapFunction)}.
     * {@linkplain MapFunction A map function} is applied to the values in this column.
     *
     * @param mapFunction function applied to each value
     * @see #map(MapFunction)
     */
    protected abstract void doMap(MapFunction<T> mapFunction);

    /**
     * {@linkplain MapFunction A map function} is applied to the values in this column.
     * {@link MapFunction#map(Object)} is called for each value.
     * <p>Calls{@link #notifyDataFrameColumnChanged()} to ensure data frame index consistency</p>
     *
     * @param mapFunction The function applied to each value
     * @return <tt>self</tt> for method chaining
     */
    public final C map(MapFunction<T> mapFunction) {
        doMap(mapFunction);
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Used by {@link #reverse()} to reverse the order of this column
     */
    protected abstract void doReverse();

    /**
     * Reverses the order of the column values
     * <p>Calls{@link #notifyDataFrameColumnChanged()} to ensure data frame index consistency</p>
     *
     * @return <tt>self</tt> for method chaining
     */
    public final C reverse() {
        doReverse();
        notifyDataFrameColumnChanged();
        return getThis();
    }


    /**
     * Returns the value at a specified index
     *
     * @param index Index of the returned value
     * @return Value at the specified index
     */
    public abstract T get(int index);

    /**
     * Creates a copy of this column
     *
     * @return The copy of this column
     */
    public abstract C copy();

    /**
     * Creates a empty copy (no values) of this column
     *
     * @return The copy of this column
     */
    public abstract C copyEmpty();

    /**
     * Clears this column.
     * All values are removed and the size is doSet to 0
     */
    public abstract void clear();


    /**
     * Copies the values of this column to a specified array of the same type.
     * The size of the array and the column size must be equal.
     *
     * @param a The array the values are copied to
     * @return the array with the copied values.
     */
    public abstract T[] toArray(T[] a);

    /**
     * Returns an {@link Comparable} array that contains all values of this column.
     *
     * @return Object array with all column values
     */
    public abstract Comparable[] toArray();

    /**
     * Returns <tt>true</tt> if <b>all</b> values of a specified collection are present in this column.
     *
     * @param c Collection that contains the tested values
     * @return <tt>true</tt> if all values are present in this column
     */
    public abstract boolean containsAll(Collection<?> c);

    /**
     * Returns <tt>true</tt> if the specified value exists in this column.
     *
     * @param o value that is tested
     * @return <tt>true</tt> if this column contains the specified value
     */
    public abstract boolean contains(T o);

    /**
     * Used by {@link #append(Comparable)} to append a value to this column.
     *
     * @param value value to be appended
     * @return <tt>true</tt> if the value is successfully appended
     */
    protected abstract boolean doAppend(T value);

    /**
     * returns true if the input value is compatible with this column
     * @param value tested value
     * @return true if value is compatible
     */
    public abstract boolean isValueValid(Comparable value);

    public abstract<H> T getValueFromRow(Row<?,H> row,H headerName);

    public abstract T getValueFromRow(Row<?,?> row, int headerIndex);

    /**
     * A new value is appended at the end of this column using {@link #doAppend(Comparable)}.
     * <p>Calls{@link #validateAppend()} to ensure data frame index consistency</p>
     *
     * @param value value to be appended
     * @return <tt>true</tt> if the value is successfully appended
     * @see #validateAppend()
     */
    public final boolean append(T value) {
        try {
            validateAppend();
        } catch (DataFrameException e) {
            log.warn(ERROR_APPENDING, e);
            return false;
        }
        return doAppend(value);
    }

    /**
     * A new value is appended at the end of this column using {@link #doAppend(Comparable)}.
     * <p>Calls{@link #validateAppend()} to ensure data frame index consistency</p>
     * @param <H> header value type
     * @param row row containing the value
     * @param headerName headerName of the value within the row
     * @return <tt>true</tt> if the value is successfully appended
     * @see #validateAppend()
     */
    public final<H> boolean append(Row<?,H> row,H headerName) {
        try {
            validateAppend();
        } catch (DataFrameException e) {
            log.warn(ERROR_APPENDING, e);
            return false;
        }
        return doAppend(getValueFromRow(row,headerName));
    }

    /**
     * A new value is appended at the end of this column using {@link #doAppend(Comparable)}.
     * <p>Calls{@link #validateAppend()} to ensure data frame index consistency</p>
     *
     * @param row row containing the value
     * @param index index of the value within the row
     * @return <tt>true</tt> if the value is successfully appended
     * @see #validateAppend()
     */
    public final boolean append(Row<?,?> row,int index) {
        try {
            validateAppend();
        } catch (DataFrameException e) {
            log.warn(ERROR_APPENDING, e);
            return false;
        }
        return doAppend(getValueFromRow(row,index));
    }

    /**
     * Append a value at the end of this column.
     * Value is first casted to the correct type.
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param value value to be appended
     * @return <tt>true</tt> if the value is successfully appended
     * @see #append(Comparable)
     */
    @SuppressWarnings("unchecked")
    protected final boolean append(Object value) {
        try {
            return append((T) value);
        } catch (Exception e) {
            log.warn(ERROR_APPENDING, e);
            throw new DataFrameRuntimeException(ERROR_APPENDING, e);
        }
    }

    /**
     * Used by {@link #appendAll(Collection)}} to append a collection of values.
     *
     * @param c collection containing values to be added to this column
     * @return <tt>true</tt> if the value is successfully appended
     * @see #append(Comparable)
     */
    protected abstract boolean doAppendAll(Collection<? extends T> c);

    /**
     * Appends all values of in a collection to this column.
     * <p>Calls {@link #validateAppend()} to ensure data frame index consistency</p>
     *
     * @param c collection containing values to be added to this column
     * @return <tt>true</tt> if all values are appended successfully
     * @see #append(Comparable)
     */
    public final boolean appendAll(Collection<? extends T> c) {
        try {
            validateAppend();
        } catch (DataFrameException e) {
            log.warn(ERROR_APPENDING, e);
            return false;
        }
        return doAppendAll(c);
    }

    /**
     * Used by {@link #appendNA()} to append NA at the end of this column.
     *
     * @return <tt>true</tt> if value was appended successfully
     * @see #appendNA()
     */
    protected abstract boolean doAppendNA();


    /**
     * Appends a {@link Values#NA NA value} to the end of this column.
     * <p>Calls {@link #validateAppend()} to ensure data frame index consistency</p>
     *
     * @return <tt>true</tt> if value was appended successfully
     */
    public final boolean appendNA() {
        try {
            validateAppend();
        } catch (DataFrameException e) {
            log.warn("error appending NA to column", e);
            return false;
        }
        return doAppendNA();
    }

    /**
     * Returns the number of values in this column.
     *
     * @return Number of values in this column
     */
    public abstract int size();


    /**
     * Returns <tt>true</tt> if this column contains no values.
     *
     * @return <tt>true</tt> if this column is empty.
     */
    public abstract boolean isEmpty();


    /**
     * Returns <tt>true</tt> if the value at the specified index equals {@link Values#NA NA}.
     *
     * @param index index to be tested for <tt>NA</tt>
     * @return <tt>true</tt> if the specified index is <tt>NA</tt>
     */
    public abstract boolean isNA(int index);

    /**
     * Used by {@link #setNA(int)} to set a specified index to {@link Values#NA NA}.
     *
     * @param index index to be doSet to <tt>NA</tt>
     */
    protected abstract void doSetNA(int index);

    /**
     * Sets the value at a specified index to {@link Values#NA NA}.
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param index index to be set NA
     * @return <tt>self</tt> for method chaining
     */
    public final C setNA(int index) {
        doSetNA(index);
        notifyDataFrameValueChanged(index);
        return getThis();
    }




    /**
     * Throws an {@link DataFrameException exception} if appending is currently not allowed.
     * Appending is not allowed if the column is part of a {@link DefaultDataFrame data frame}.
     * This step is necessary to ensure consistency of indices in the data frame.
     *
     * @throws DataFrameException Exception thrown if appending is not allowed
     */
    public void validateAppend() throws DataFrameException {
        if (!dataFrameAppend && getDataFrame() != null) {
            throw new DataFrameException("doAppend can only be used if the column is not added to a data frame. use dataFrame.append()");
        }
    }

    /**
     * Notifies the parent {@link DefaultDataFrame data frame} about a value change at a specified index.
     * This tells the data frame to update the indices if required.
     *
     * @param index index of the changed value
     */
    public void notifyDataFrameValueChanged(int index) {
        if (dataFrame == null) {
            return;
        }
        dataFrame.notifyColumnValueChanged(this, index, get(index));
    }

    /**
     * Notifies the parent {@link DefaultDataFrame data frame} that this column changed.
     * This tells the data frame to update the indices if required.
     */
    public void notifyDataFrameColumnChanged() {
        if (dataFrame == null) {
            return;
        }
        dataFrame.notifyColumnChanged(this);
    }


    /**
     * Internal method used by the parent data frame to enable appending.
     */
    protected void startDataFrameAppend() {
        this.dataFrameAppend = true;
    }

    /**
     * Internal method used by the parent data frame to disable appending.
     */
    protected void endDataFrameAppend() {
        this.dataFrameAppend = false;
    }

    /**
     * Returns the parent {@link DefaultDataFrame data frame}
     *
     * @return parent data frame
     */
    public DataFrame getDataFrame() {
        return dataFrame;
    }

    /**
     * Used internally by the data frame. Sets the parent {@link DefaultDataFrame data frame}.
     * Columns can only be part of one data frame.
     * Throws an exception if this column is already appended to an other data frame.
     *
     * @param dataFrame parent data frame
     * @throws DataFrameException thrown if column already assigned to other data frame
     */
    protected void setDataFrame(DefaultDataFrame dataFrame) throws DataFrameException {
        if (dataFrame == null) {
            this.dataFrame = null;
            return;
        }
        if (!dataFrame.containsColumn(this)) {
            throw new DataFrameException("setDataFrame is only used internally. please use dataFrame.addColumn");
        }
        this.dataFrame = dataFrame;
    }


}
