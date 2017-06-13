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
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.GroupUtil;
import de.unknownreality.dataframe.join.JoinUtil;

import java.util.LinkedHashMap;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameBuilder {
    private final LinkedHashMap<String, DataFrameColumn> columns = new LinkedHashMap<>();

    private JoinUtil joinUtil = null;
    private GroupUtil groupUtil = null;
    private  DataContainer<?, ?> dataContainer;
    private FilterPredicate filterPredicate = FilterPredicate.EMPTY_FILTER;
    protected DataFrameBuilder() {
    }

    protected DataFrameBuilder(DataContainer<?,?> container) {
        this.dataContainer = container;
    }
    public static DataFrame createDefault(){
        return new DefaultDataFrame();
    }

    /**
     * Creates a data frame builder instance based on a parent data container.
     *
     * @param dataContainer parent data container
     * @return data frame builder
     */
    public static DataFrameBuilder createFrom(DataContainer dataContainer) {
        return new DataFrameBuilder(dataContainer);
    }

    public static DataFrameBuilder create(){
        return new DataFrameBuilder();
    }

    /**
     * Adds a new column to the builder.
     *
     * @param column data frame column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addColumn(DataFrameColumn column) {
        columns.put(column.getName(), column);
        return this;
    }

    /**
     * Adds a new {@link BooleanColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addBooleanColumn(String name) {
        BooleanColumn column = new BooleanColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link ByteColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addByteColumn(String name) {
        ByteColumn column = new ByteColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link DoubleColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addDoubleColumn(String name) {
        DoubleColumn column = new DoubleColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link FloatColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addFloatColumn(String name) {
        FloatColumn column = new FloatColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link IntegerColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addIntegerColumn(String name) {
        IntegerColumn column = new IntegerColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link LongColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addLongColumn(String name) {
        LongColumn column = new LongColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link ShortColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addShortColumn(String name) {
        ShortColumn column = new ShortColumn(name);
        return addColumn(column);
    }

    /**
     * Adds a new {@link StringColumn} to the builder.
     *
     * @param name name of the column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addStringColumn(String name) {
        StringColumn column = new StringColumn(name);
        return addColumn(column);
    }


    public DataFrameBuilder setGroupUtil(GroupUtil groupUtil) {
        this.groupUtil = groupUtil;
        return this;
    }

    public DataFrameBuilder setJoinUtil(JoinUtil joinUtil) {
        this.joinUtil = joinUtil;
        return this;
    }

    public DataFrameBuilder withFilterPredicate(FilterPredicate predicate){
        this.filterPredicate = predicate;
        return this;
    }

    public DataFrameBuilder from(DataContainer<?, ?> container){
        this.dataContainer = container;
        return this;
    }


    /**
     * Adds a new column to the builder and defines the name of the column in the parent data container.
     *
     * @param header column name in the parent data container
     * @param column data frame column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameBuilder addColumn(String header, DataFrameColumn column) {
        columns.put(header, column);
        return this;
    }

    public LinkedHashMap<String, DataFrameColumn> getColumns() {
        return columns;
    }

    /**
     * Builds a new data frame.
     *
     * @return created data frame
     */
    public DataFrame build() {
        if(dataContainer != null){
            return DataFrameConverter.fromDataContainer(dataContainer, getColumns(),filterPredicate);
        }
        DefaultDataFrame dataFrame = new DefaultDataFrame();
        for(String n : columns.keySet()){
            DataFrameColumn col = columns.get(n);
            col.setName(n);
            dataFrame.addColumn(col);
        }
        if(joinUtil != null){
            dataFrame.setJoinUtil(joinUtil);
        }
        if(groupUtil != null){
            dataFrame.setGroupUtil(groupUtil);
        }
        return dataFrame;
    }

}
