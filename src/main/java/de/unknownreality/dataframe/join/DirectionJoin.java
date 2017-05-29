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
import de.unknownreality.dataframe.group.DataGroup;
import de.unknownreality.dataframe.group.DataGrouping;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 10.07.2016.
 */
public abstract class DirectionJoin extends AbstractJoin {
    /**
     * Creates a direction (left or right) join
     *
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinHeader  joined data frame header
     * @param joinInfo    info about the columns in the joined data frame
     * @param joinColumns columns used for the join
     * @return joined data frame
     */
    public JoinedDataFrame createDirectionJoin(DataFrame dfA, DataFrame dfB,
                                               DataFrameHeader joinHeader, JoinInfo joinInfo, JoinColumn[] joinColumns) {
        String[] groupColumns = new String[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            groupColumns[i] = joinColumns[i].getColumnB();
        }
        List<DataRow> joinedRows = new ArrayList<>();
        Comparable[] groupValues = new Comparable[joinColumns.length];
        DataGrouping joinedGroups = dfB.groupBy(groupColumns);
        for (DataRow row : dfA) {
            if (joinInfo.isA(dfA)) {
                setGroupValuesA(groupValues, row, joinColumns);
            } else {
                setGroupValuesB(groupValues, row, joinColumns);
            }
            DataGroup group = joinedGroups.findByGroupValues((Comparable[]) groupValues);
            if (group == null) {
                Comparable[] joinedRowValues = new Comparable[joinHeader.size()];
                fillValues(dfA, row, joinInfo, joinedRowValues);
                fillNA(joinedRowValues);
                DataRow joinedRow = new DataRow(joinHeader, joinedRowValues, joinedRows.size());
                joinedRows.add(joinedRow);
            } else {
                appendGroupJoinedRows(group, dfA, dfB, row, joinInfo, joinHeader, joinedRows);
            }
        }
        JoinedDataFrame joinedDataFrame = new JoinedDataFrame(joinInfo);
        joinedDataFrame.set(joinHeader, joinedRows);
        return joinedDataFrame;
    }


}
