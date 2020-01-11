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
import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinInfo;

import java.util.*;

/**
 * Created by Alex on 10.07.2016.
 */
public class JoinOperationUtil {


    /**
     * Fills the values of a row into the corresponding values in a joined row value
     *
     * @param joinIndices     indices of column from original dataframe in joined dataframe
     * @param row             row containing the values
     * @param joinedRowValues array that is filled with the row values
     */

    public static void fillValues(int[] joinIndices, DataRow row, Comparable<?>[] joinedRowValues) {
        for (int i = 0; i < joinIndices.length; i++) {
            joinedRowValues[joinIndices[i]] = row.get(i);

        }
    }

    public static int[] getJoinIndices(DataFrame dataFrame, JoinInfo joinInfo) {
        int[] joindIndices = new int[dataFrame.getHeader().size()];
        int i = 0;
        for (String headerName : dataFrame.getHeader()) {
            int joinedIndex = joinInfo.getJoinedIndex(headerName, dataFrame);
            joindIndices[i++] = joinedIndex;
        }
        return joindIndices;
    }

    /**
     * Fills an value array with {@link Values#NA}
     *
     * @param joinedRowValues array with <tt>NA</tt> values
     */
    public static void fillNA(Comparable<?>[] joinedRowValues) {
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
     * @param dfA         first data frame
     * @param dfB         second data frame
     * @param joinColumns columns used for the join
     * @param suffixA     suffix used for headers from the first data frame
     * @param suffixB     suffix used for headers from the second data frame
     * @return information about the join
     */
    public static JoinInfo createJoinInfo(DataFrame dfA, DataFrame dfB,
                                          JoinColumn[] joinColumns, String suffixA, String suffixB) {
        DataFrameHeader joinHeader = new DataFrameHeader();
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
     * Appends the joined rows resulting from a row from one data frame and a data group from the other data frame
     *
     * @param rowIndices      row indices
     * @param dfB             second data frame
     * @param rowA            row from the other data frame  (not the same as the data group)
     * @param joinIndicesA    indices of column from first dataframe in joined dataframe
     * @param joinIndicesB    indices of column from second dataframe in joined dataframe
     * @param joinedSize      size of joined rows
     * @param joinedDataFrame resulting joined data frame
     */

    public static void appendGroupJoinedRows(Collection<Integer> rowIndices, DataFrame dfB,
                                             DataRow rowA, int[] joinIndicesA, int[] joinIndicesB, int joinedSize, DataFrame joinedDataFrame) {
        for (Integer rowB : rowIndices) {
            Comparable<?>[] joinedRowValues = new Comparable<?>[joinedSize];
            fillValues(joinIndicesA, rowA, joinedRowValues);
            fillValues(joinIndicesB, dfB.getRow(rowB), joinedRowValues);
            fillNA(joinedRowValues);
            joinedDataFrame.append(joinedRowValues);
        }
    }
}
