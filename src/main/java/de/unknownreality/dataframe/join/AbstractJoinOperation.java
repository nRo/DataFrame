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

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.group.DataGroup;

import java.util.*;

/**
 * Created by Alex on 10.07.2016.
 */
public abstract class AbstractJoinOperation {

    public abstract JoinedDataFrame join(DataFrame<?,?> dfA, DataFrame<?,?> dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns);


    /**
     * Fills the values of a row into the corresponding values in a joined row value
     *
     * @param dataFrame       parent data frame of row
     * @param row             row containing the values
     * @param joinInfo        info about the joined data frame
     * @param joinedRowValues array that is filled with the row values
     */
    public void fillValues(DataFrame<?,?> dataFrame, DataRow row, JoinInfo joinInfo, Comparable[] joinedRowValues) {
        for (String headerName : dataFrame.getHeader()) {
            int joinedIndex = joinInfo.getJoinedIndex(headerName, dataFrame);
            joinedRowValues[joinedIndex] = row.get(headerName);
        }
    }

    /**
     * Fills an value array with {@link Values#NA}
     *
     * @param joinedRowValues array with <tt>NA</tt> values
     */
    public void fillNA(Comparable[] joinedRowValues) {
        for (int i = 0; i < joinedRowValues.length; i++) {
            if (joinedRowValues[i] == null) {
                joinedRowValues[i] = Values.NA;
            }
        }
    }

    /**
     * Fills a join header with the header information of two data frames.<br>
     * Headers that are used for the join are merged.<br>
     * Headers with the same name in both data frames get a suffix.<br>
     * The information about the join is returned after the header is filled.
     *
     * @param joinHeader  join header to fill
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinColumns columns used for the join
     * @param suffixA     suffix used for headers from the first data frame
     * @param suffixB     suffix used for headers from the second data frame
     * @return information about the join
     */
    public JoinInfo fillJoinHeader(DataFrameHeader joinHeader, DataFrame<?,?> dfA, DataFrame<?,?> dfB,
                                   JoinColumn[] joinColumns, String suffixA, String suffixB) {
        Set<String> joinColumnSetA = new HashSet<>();
        Set<String> joinColumnSetB = new HashSet<>();
        Map<String, String> joinedBToAMap = new HashMap<>();
        for (JoinColumn column : joinColumns) {
            joinColumnSetA.add(column.getColumnA());
            joinColumnSetB.add(column.getColumnB());
            joinedBToAMap.put(column.getColumnB(), column.getColumnA());
        }
        JoinInfo info = new JoinInfo(joinHeader, dfA, dfB);

        for (String s : dfA.getHeader()) {
            String name;
            if (joinColumnSetA.contains(s)) {
                name = s;
            } else if (dfB.getHeader().contains(s)) {
                name = s + suffixA;
            } else {
                name = s;
            }
            info.addDataFrameAHeader(s, name);
            joinHeader.add(name, dfA.getHeader().getColumnType(s), dfA.getHeader().getType(s));
        }
        for (String s : dfB.getHeader()) {
            String name;
            if (joinColumnSetB.contains(s)) {
                name = joinedBToAMap.get(s);
                info.addDataFrameBHeader(s, name);
                continue;
            } else if (dfA.getHeader().contains(s)) {
                name = s + suffixB;
            } else {
                name = s;
            }
            info.addDataFrameBHeader(s, name);
            joinHeader.add(name, dfB.getHeader().getColumnType(s), dfB.getHeader().getType(s));
        }
        return info;
    }


    /**
     * Fills the join values of the first data frame into an values array
     *
     * @param groupValues values array to be filled
     * @param row         row containing the values
     * @param joinColumns columns used for the join
     */
    public void setGroupValuesA(Comparable[] groupValues, Row<Comparable,String> row, JoinColumn[] joinColumns) {
        for (int i = 0; i < joinColumns.length; i++) {
            String headerName = joinColumns[i].getColumnA();
            groupValues[i] = row.get(headerName);
        }
    }

    /**
     * Fills the join values of the second data frame into an values array
     *
     * @param groupValues values array to be filled
     * @param row         row containing the values
     * @param joinColumns columns used for the join
     */
    public void setGroupValuesB(Comparable[] groupValues, Row<Comparable,String> row, JoinColumn[] joinColumns) {
        for (int i = 0; i < joinColumns.length; i++) {
            String headerName = joinColumns[i].getColumnB();
            groupValues[i] = row.get(headerName);
        }
    }

    /**
     * Appends the joined rows resulting from a row from one data frame and a data group from the other data frame
     *
     * @param group      data group a data frame
     * @param dfA        first data frame
     * @param dfB        second data frame
     * @param rowA       row from the other data frame  (not the same as the data group)
     * @param joinInfo   info about the join
     * @param joinHeader resulting data frame header
     * @param joinedRows list of rows for the joined data frame
     */
    public void appendGroupJoinedRows(DataGroup group, DataFrame<?,?> dfA, DataFrame<?,?> dfB, DataRow rowA, JoinInfo joinInfo, DefaultDataFrameHeader joinHeader, List<DataRow> joinedRows) {
        for (DataRow rowB : group) {
            Comparable[] joinedRowValues = new Comparable[joinHeader.size()];
            fillValues(dfA, rowA, joinInfo, joinedRowValues);
            fillValues(dfB, rowB, joinInfo, joinedRowValues);
            fillNA(joinedRowValues);
            DataRow joinedRow = new DataRow(joinHeader, joinedRowValues, joinedRows.size());
            joinedRows.add(joinedRow);
        }
    }
}
