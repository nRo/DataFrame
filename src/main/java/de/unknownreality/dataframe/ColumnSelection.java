/*
 *
 *  * Copyright (c) 2019 Alexander Grün
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

import de.unknownreality.dataframe.filter.FilterPredicate;

public class ColumnSelection {
    private final DataFrameColumn<?, ?>[] columns;
    private final DataFrame dataFrame;

    public ColumnSelection(DataFrame dataFrame, DataFrameColumn<?, ?>... columns) {
        this.dataFrame = dataFrame;
        this.columns = columns;
    }

    /**
     * Returns a dataframe containing the selected columns and rows where a specified column value equals
     * an input value.
     *
     * @param colName column name
     * @param value   input value
     * @return new dataframe
     */
    public DataFrame where(String colName, Object value) {
        DataRows rows = dataFrame.selectRows(colName, value);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and rows filtered by a{@link FilterPredicate}
     * an input value.
     *
     * @param predicate input predicate
     * @return new dataframe
     */
    public DataFrame where(FilterPredicate predicate){
        DataRows rows = dataFrame.selectRows(predicate);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and rows filtered by a predicate
     * an input value.
     *
     * @param predicateString input predicate string
     * @return new dataframe
     */
    public DataFrame where(String predicateString){
        DataRows rows = dataFrame.selectRows(predicateString);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and rows found using a specified index
     * an input value.
     *
     * @param indexName name of index
     * @param values    index values
     * @return new dataframe
     */
    public DataFrame whereIndex(String indexName, Object... values) {
        DataRows rows = dataFrame.selectRowsByIndex(indexName, values);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and all rows from the original dataframe.
     *
     * @return new dataframe
     */
    public DataFrame allRows(){
        DataRows rows = dataFrame.getRows();
        return createDataFrame(rows);
    }

    private DataFrame createDataFrame(DataRows rows){
        DataFrame df = new DefaultDataFrame();
        for (DataFrameColumn<?, ?> column : columns) {
            df.addColumn(column.copyEmpty());
        }
        DataRows newRows = new DataRows(df,rows);
        df.set(newRows);
        return df;
    }


}