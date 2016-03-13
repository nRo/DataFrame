package de.unknownreality.data.frame.join;

import de.unknownreality.data.common.Row;
import de.unknownreality.data.frame.DataFrame;
import de.unknownreality.data.frame.DataFrameHeader;
import de.unknownreality.data.frame.DataRow;
import de.unknownreality.data.frame.Values;
import de.unknownreality.data.frame.group.DataGroup;
import de.unknownreality.data.frame.group.DataGrouping;

import java.util.*;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameJoinUtil {
    public static final String JOIN_SUFFIX_A = ".A";
    public static final String JOIN_SUFFIX_B = ".B";

    public static JoinedDataFrame leftJoin(DataFrame dfA, DataFrame dfB, JoinColumn... joinColumns) {
        return leftJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    public static JoinedDataFrame leftJoin(DataFrame dfA, DataFrame dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        DataFrameHeader joinHeader = new DataFrameHeader();
        JoinInfo joinInfo = fillJoinHeader(joinHeader, dfA, dfB, joinColumns, joinSuffixA, joinSuffixB);
        return createDirectionJoin(dfA, dfB, joinHeader, joinInfo, joinColumns);
    }

    public static JoinedDataFrame rightJoin(DataFrame dfA, DataFrame dfB, JoinColumn... joinColumns) {
        return rightJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    public static JoinedDataFrame rightJoin(DataFrame dfA, DataFrame dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        DataFrameHeader joinHeader = new DataFrameHeader();
        JoinInfo joinInfo = fillJoinHeader(joinHeader, dfA, dfB, joinColumns, joinSuffixA, joinSuffixB);
        return createDirectionJoin(dfB, dfA, joinHeader, joinInfo, joinColumns);
    }

    public static JoinedDataFrame innerJoin(DataFrame dfA, DataFrame dfB, JoinColumn... joinColumns) {
        return innerJoin(dfA, dfB, JOIN_SUFFIX_A, JOIN_SUFFIX_B, joinColumns);
    }

    public static JoinedDataFrame innerJoin(DataFrame dfA, DataFrame dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns) {
        DataFrameHeader joinHeader = new DataFrameHeader();
        JoinInfo joinInfo = fillJoinHeader(joinHeader, dfA, dfB, joinColumns, joinSuffixA, joinSuffixB);
        return createInnerJoin(dfA, dfB, joinHeader, joinInfo, joinColumns);
    }

    private static JoinedDataFrame createInnerJoin(DataFrame dfA, DataFrame dfB,
                                                   DataFrameHeader joinHeader, JoinInfo joinInfo, JoinColumn[] joinColumns) {
        String[] groupColumns = new String[joinColumns.length];
        for (int i = 0; i < joinColumns.length; i++) {
            groupColumns[i] = joinColumns[i].getColumnB();
        }
        List<DataRow> joinedRows = new ArrayList<>();
        Comparable[] groupValues = new Comparable[joinColumns.length];
        DataGrouping joinedGroups = dfB.groupBy(groupColumns);
        for (DataRow row : dfA) {
            setGroupValuesA(groupValues, row, joinColumns);
            DataGroup group = joinedGroups.findByGroupValues(groupValues);
            if (group == null) {
                continue;
            } else {
                for (DataRow rowB : group) {
                    Comparable[] joinedRowValues = new Comparable[joinHeader.size()];
                    fillValues(dfA, row, joinInfo, joinedRowValues);
                    fillValues(dfB, rowB, joinInfo, joinedRowValues);
                    fillNA(joinedRowValues);
                    DataRow joinedRow = new DataRow(joinHeader, joinedRowValues, joinedRows.size());
                    joinedRows.add(joinedRow);
                }
            }
        }
        JoinedDataFrame joinedDataFrame = new JoinedDataFrame(joinInfo);
        joinedDataFrame.set(joinHeader, joinedRows);
        return joinedDataFrame;
    }

    private static JoinedDataFrame createDirectionJoin(DataFrame dfA, DataFrame dfB,
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
            DataGroup group = joinedGroups.findByGroupValues(groupValues);
            if (group == null) {
                Comparable[] joinedRowValues = new Comparable[joinHeader.size()];
                fillValues(dfA, row, joinInfo, joinedRowValues);
                fillNA(joinedRowValues);
                DataRow joinedRow = new DataRow(joinHeader, joinedRowValues, joinedRows.size());
                joinedRows.add(joinedRow);
            } else {
                for (DataRow rowB : group) {
                    Comparable[] joinedRowValues = new Comparable[joinHeader.size()];
                    fillValues(dfA, row, joinInfo, joinedRowValues);
                    fillValues(dfB, rowB, joinInfo, joinedRowValues);
                    fillNA(joinedRowValues);
                    DataRow joinedRow = new DataRow(joinHeader, joinedRowValues, joinedRows.size());
                    joinedRows.add(joinedRow);
                }
            }
        }
        JoinedDataFrame joinedDataFrame = new JoinedDataFrame(joinInfo);
        joinedDataFrame.set(joinHeader, joinedRows);
        return joinedDataFrame;
    }


    private static void fillValues(DataFrame dataFrame, DataRow row, JoinInfo joinInfo, Comparable[] joinedRowValues) {
        for (String headerName : dataFrame.getHeader()) {
            int joinedIndex = joinInfo.getJoinedIndex(headerName, dataFrame);
            joinedRowValues[joinedIndex] = row.get(headerName);
        }
    }


    private static void fillNA(Comparable[] joinedRowValues) {
        for (int i = 0; i < joinedRowValues.length; i++) {
            if (joinedRowValues[i] == null) {
                joinedRowValues[i] = Values.NA;
            }
        }
    }

    private static JoinInfo fillJoinHeader(DataFrameHeader joinHeader, DataFrame dfA, DataFrame dfB,
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


    private static void setGroupValuesA(Comparable[] groupValues, Row<Comparable> row, JoinColumn[] joinColumns) {
        for (int i = 0; i < joinColumns.length; i++) {
            String headerName = joinColumns[i].getColumnA();
            groupValues[i] = row.get(headerName);
        }
    }

    private static void setGroupValuesB(Comparable[] groupValues, Row<Comparable> row, JoinColumn[] joinColumns) {
        for (int i = 0; i < joinColumns.length; i++) {
            String headerName = joinColumns[i].getColumnA();
            groupValues[i] = row.get(headerName);
        }
    }

}
