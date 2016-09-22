/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.filter.FilterPredicate;

import java.util.LinkedHashMap;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameBuilder {
    private final LinkedHashMap<String, DataFrameColumn> columns = new LinkedHashMap<>();
    private final DataContainer<?, ?> dataContainer;
    private FilterPredicate filterPredicate = FilterPredicate.EMPTY;
    private DataFrameBuilder(DataContainer dataContainer) {
        this.dataContainer = dataContainer;
    }

    /**
     * Creates a data frame builder instance based on a parent data container.
     *
     * @param dataContainer parent data container
     * @return data frame builder
     */
    public static DataFrameBuilder create(DataContainer dataContainer) {
        return new DataFrameBuilder(dataContainer);
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

    public DataFrameBuilder withFilterPredicate(FilterPredicate predicate){
        this.filterPredicate = predicate;
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


    /**
     * Builds the data frame using the parent data container and column information.
     *
     * @return created data frame
     * @see DataFrameConverter#fromDataContainer(DataContainer, LinkedHashMap)
     */
    public DataFrame build() {
        return DataFrameConverter.fromDataContainer(dataContainer, columns,filterPredicate);

    }


}
