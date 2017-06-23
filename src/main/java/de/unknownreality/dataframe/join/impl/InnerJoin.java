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

package de.unknownreality.dataframe.join.impl;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupRow;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinInfo;
import de.unknownreality.dataframe.join.JoinedDataFrame;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 10.07.2016.
 */
public class InnerJoin extends AbstractJoinOperation {
    protected InnerJoin() {
    }


    /**
     * Joins two data frames using the <tt>INNER JOIN</tt> method
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
        String[] groupColumns = new String[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            groupColumns[i] = joinColumns[i].getColumnB();
        }
        List<DataRow> joinedRows = new ArrayList<>();
        Comparable[] groupValues = new Comparable[joinColumns.length];
        DataGrouping joinedGroups = dfB.groupBy(groupColumns);
        for (DataRow row : dfA) {
            setGroupValuesA(groupValues, row, joinColumns);
            GroupRow groupRow = joinedGroups.findByGroupValues((Comparable[]) groupValues);
            if (groupRow != null) {
                appendGroupJoinedRows(groupRow.getGroup(), dfA, dfB, row, joinInfo, joinHeader, joinedRows);
            }
        }
        JoinedDataFrame joinedDataFrame = new JoinedDataFrame(joinInfo);
        joinedDataFrame.set(joinHeader, joinedRows);
        return joinedDataFrame;
    }


}
