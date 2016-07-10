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

package de.unknownreality.dataframe.join;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameHeader;

/**
 * Created by Alex on 10.07.2016.
 */
public class RightJoin extends DirectionJoin {
    protected RightJoin() {
    }

    /**
     * Joins two data frames using the <tt>RIGHT JOIN</tt> method
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinSuffixA suffix used for columns from the first data frame
     * @param joinSuffixB suffix used for columns from the second data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     */
    @Override
    public JoinedDataFrame join(DataFrame dfA, DataFrame dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        DataFrameHeader joinHeader = new DataFrameHeader();
        JoinInfo joinInfo = fillJoinHeader(joinHeader, dfA, dfB, joinColumns, joinSuffixA, joinSuffixB);
        return createDirectionJoin(dfB, dfA, joinHeader, joinInfo, joinColumns);
    }
}
