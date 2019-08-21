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

package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataRow;

import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 27.05.2016.
 */
public interface Index {
    /**
     * updates a {@link DataRow} in this index
     *
     * @param dataRow data row to update
     */
    void update(DataRow dataRow);


    /**
     * removes a {@link DataRow} from this index
     *
     * @param dataRow data row to remove
     */
    void remove(DataRow dataRow);

    /**
     * Returns the row number for values used in this index
     *
     * @param values indexed row values
     * @return index of row with the input value
     */
    Collection<Integer> find(Comparable... values);

    /**
     * Returns the name if this index
     *
     * @return name of index
     */
    String getName();

    /**
     * set true if only unique values are allowed for this index
     * @param unique allow only unique values
     */
    void setUnique(boolean unique);


    /**
     * Returns <tt>true</tt> if this index contains the specified column
     *
     * @param column column to tes t
     * @return <tt>true</tt> if column is contained in this index
     */
    boolean containsColumn(DataFrameColumn column);

    /**
     * Returns <tt>true</tt> if this index is unique
     * @return <tt>true</tt> if this index is unique
     */
    boolean isUnique();
    /**
     * Returns the columns used in this index
     *
     * @return columns in this index
     */
    List<DataFrameColumn> getColumns();

    /**
     * Clears this index
     */
    void clear();

    void replaceColumn(DataFrameColumn existing, DataFrameColumn replacement);

}
