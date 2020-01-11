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

package de.unknownreality.dataframe.join.impl;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinUtil;
import de.unknownreality.dataframe.join.JoinedDataFrame;

/**
 * Created by Alex on 12.03.2016.
 */
public class DefaultJoinUtil implements JoinUtil {
    public static final String JOIN_SUFFIX_A = ".A";
    public static final String JOIN_SUFFIX_B = ".B";

    /**
     * RIGHT JOIN
     */
    public static final RightJoin RIGHT = new RightJoin();

    /**
     * LEFT JOIN
     */
    public static final LeftJoin LEFT = new LeftJoin();

    /**
     * INNER JOIN
     */
    public static final InnerJoin INNER = new InnerJoin();

    /**
     * INNER JOIN
     */
    public static final OuterJoin OUTER = new OuterJoin();

    /**
     * Joins two data frames using the <tt>LEFT JOIN</tt> method and the default header name suffixes
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see LeftJoin#join(DataFrame , DataFrame , String, String, JoinColumn...)
     */
    public JoinedDataFrame leftJoin(DataFrame dfA, DataFrame dfB, JoinColumn... joinColumns) {
        return leftJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    /**
     * Joins two data frames using the <tt>LEFT JOIN</tt> method and specified suffixes for the column header names
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinSuffixA suffix used for columns in the first data frame
     * @param joinSuffixB suffix used for columns in the second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see LeftJoin#join(DataFrame , DataFrame , String, String, JoinColumn...)
     */
    public JoinedDataFrame leftJoin(DataFrame  dfA, DataFrame  dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        return LEFT.join(dfA, dfB, joinSuffixA, joinSuffixB, joinColumns);

    }

    /**
     * Joins two data frames using the <tt>RIGHT JOIN</tt> method and the default header name suffixes
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see LeftJoin#join(DataFrame , DataFrame , String, String, JoinColumn...)
     */
    public JoinedDataFrame rightJoin(DataFrame  dfA, DataFrame  dfB, JoinColumn... joinColumns) {
        return rightJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    /**
     * Joins two data frames using the <tt>RIGHT JOIN</tt> method and specified suffixes for the column header names
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinSuffixA suffix used for columns in the first data frame
     * @param joinSuffixB suffix used for columns in the second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see LeftJoin#join(DataFrame , DataFrame , String, String, JoinColumn...)
     */
    public JoinedDataFrame rightJoin(DataFrame  dfA, DataFrame  dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        return RIGHT.join(dfA, dfB, joinSuffixA, joinSuffixB, joinColumns);

    }



    /**
     * Joins two data frames using the <tt>INNER JOIN</tt> method and the default header name suffixes
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see InnerJoin#join(DataFrame , DataFrame , String, String, JoinColumn...)
     */
    public JoinedDataFrame innerJoin(DataFrame  dfA, DataFrame  dfB, JoinColumn... joinColumns) {
        return innerJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    /**
     * Joins two data frames using the <tt>INNER JOIN</tt> method and specified suffixes for the column header names
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinSuffixA suffix used for columns in the first data frame
     * @param joinSuffixB suffix used for columns in the second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see InnerJoin#join(DataFrame , DataFrame, String, String, JoinColumn...)
     */
    public JoinedDataFrame innerJoin(DataFrame  dfA, DataFrame  dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        return INNER.join(dfA, dfB, joinSuffixA, joinSuffixB, joinColumns);
    }

    /**
     * Joins two data frames using the <tt>OUTER JOIN</tt> method and the default header name suffixes
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see InnerJoin#join(DataFrame , DataFrame , String, String, JoinColumn...)
     */
    public JoinedDataFrame outerJoin(DataFrame  dfA, DataFrame  dfB, JoinColumn... joinColumns) {
        return outerJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    /**
     * Joins two data frames using the <tt>OUTER JOIN</tt> method and specified suffixes for the column header names
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinSuffixA suffix used for columns in the first data frame
     * @param joinSuffixB suffix used for columns in the second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     * @see InnerJoin#join(DataFrame , DataFrame, String, String, JoinColumn...)
     */
    public JoinedDataFrame outerJoin(DataFrame  dfA, DataFrame  dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        return OUTER.join(dfA, dfB, joinSuffixA, joinSuffixB, joinColumns);
    }

}
