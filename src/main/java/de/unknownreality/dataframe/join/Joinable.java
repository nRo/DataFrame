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

package de.unknownreality.dataframe.join;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.common.Header;

/**
 * Created by Alex on 12.06.2017.
 */
public interface Joinable<H extends DataFrameHeader<H>, R extends DataRow> {
    DataFrame<H,R> getThis();

    JoinUtil getJoinUtil();

    /**
     * Joins this data frame with another data frame using the <tt>LEFT JOIN</tt> method.
     *
     * @param dataFrame   other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#leftJoin(DataFrame, DataFrame, JoinColumn...)
     */
    default JoinedDataFrame joinLeft(DataFrame<H,R> dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinLeft(dataFrame, joinColumnsArray);
    }

    /**
     * Joins this data frame with another data frame using the <tt>LEFT JOIN</tt> method.
     *
     * @param dataFrame   other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#leftJoin(DataFrame, DataFrame, JoinColumn...)
     */
    default JoinedDataFrame joinLeft(DataFrame<H,R> dataFrame, JoinColumn... joinColumns) {
        return getJoinUtil().leftJoin(getThis(), dataFrame, joinColumns);
    }

    /**
     * Joins this data frame with another data frame using the <tt>LEFT JOIN</tt> method.
     * Column names are altered using the provided suffixes.
     *
     * @param dataFrame   other data frame
     * @param suffixA     suffixes for columns from this data frame
     * @param suffixB     suffixes for columns from the other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#leftJoin(DataFrame, DataFrame, String, String, JoinColumn...)
     */

    default JoinedDataFrame joinLeft(DataFrame<H,R> dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return getJoinUtil().leftJoin(getThis(), dataFrame, suffixA, suffixB, joinColumns);
    }

    /**
     * Joins this data frame with another data frame using the <tt>RIGHT JOIN</tt> method.
     *
     * @param dataFrame   other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#rightJoin(DataFrame, DataFrame, JoinColumn...)
     */
    default JoinedDataFrame joinRight(DataFrame<H,R> dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinRight(dataFrame, joinColumnsArray);
    }

    /**
     * Joins this data frame with another data frame using the <tt>LEFT JOIN</tt> method.
     *
     * @param dataFrame   other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#leftJoin(DataFrame, DataFrame, JoinColumn...)
     */
    default JoinedDataFrame joinRight(DataFrame<H,R> dataFrame, JoinColumn... joinColumns) {
        return getJoinUtil().rightJoin(getThis(), dataFrame, joinColumns);
    }

    /**
     * Joins this data frame with another data frame using the <tt>RIGHT JOIN</tt> method.
     * Column names are altered using the provided suffixes.
     *
     * @param dataFrame   other data frame
     * @param suffixA     suffixes for columns from this data frame
     * @param suffixB     suffixes for columns from the other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#rightJoin(DataFrame, DataFrame, String, String, JoinColumn...)
     */
    default JoinedDataFrame joinRight(DataFrame<H,R> dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return getJoinUtil().rightJoin(getThis(), dataFrame, suffixA, suffixB, joinColumns);
    }

    /**
     * Joins this data frame with another data frame using the <tt>INNER JOIN</tt> method.
     *
     * @param dataFrame   other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#innerJoin(DataFrame, DataFrame, JoinColumn...)
     */
    default JoinedDataFrame joinInner(DataFrame<H,R> dataFrame, String... joinColumns) {
        JoinColumn[] joinColumnsArray = new JoinColumn[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            joinColumnsArray[i] = new JoinColumn(joinColumns[i]);
        }
        return joinInner(dataFrame, joinColumnsArray);
    }

    /**
     * Joins this data frame with another data frame using the <tt>INNER JOIN</tt> method.
     *
     * @param dataFrame   other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#innerJoin(DataFrame, DataFrame, JoinColumn...)
     */
    default JoinedDataFrame joinInner(DataFrame<H,R> dataFrame, JoinColumn... joinColumns) {
        return getJoinUtil().innerJoin(getThis(), dataFrame, joinColumns);
    }

    /**
     * Joins this data frame with another data frame using the <tt>INNER JOIN</tt> method.
     * Column names are altered using the provided suffixes.
     *
     * @param dataFrame   other data frame
     * @param suffixA     suffixes for columns from this data frame
     * @param suffixB     suffixes for columns from the other data frame
     * @param joinColumns join columns
     * @return joined data frame
     * @see DefaultJoinUtil#innerJoin(DataFrame, DataFrame, String, String, JoinColumn...)
     */
    default JoinedDataFrame joinInner(DataFrame<H,R> dataFrame, String suffixA, String suffixB, JoinColumn... joinColumns) {
        return getJoinUtil().innerJoin(getThis(), dataFrame, suffixA, suffixB, joinColumns);
    }
}
