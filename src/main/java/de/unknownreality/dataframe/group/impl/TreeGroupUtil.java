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

package de.unknownreality.dataframe.group.impl;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.group.DataGroup;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Alex on 10.03.2016.
 */
public class TreeGroupUtil implements GroupUtil {
    /**
     * Groups a {@link DefaultDataFrame} using one or more columns.
     *
     * @param df      input data frame
     * @param columns grouping columns
     * @return data grouping
     */
    public DataGrouping groupBy(DataFrame df, String... columns) {
        List<DataGroup> groupList = new ArrayList<>();
        DataFrameHeader header = df.getHeader().copy();
        GroupNode root = new GroupNode(null);
        DataFrameColumn[] dfColumns = new DataFrameColumn[df.getHeader().size()];
        df.getColumns().toArray(dfColumns);
        int[] groupColumnIndices = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            groupColumnIndices[i] = header.getIndex(columns[i]);
        }
        Comparable[] groupValues = new Comparable[columns.length];
        for (int i = 0; i < df.size(); i++) {
            addRec(groupList, root, 0, columns, groupColumnIndices, groupValues, header, df, i);
        }
        root.clear();
        return new DataGrouping(groupList, createGroupColumns(df, columns));
    }

    private void addRec(List<DataGroup> groups, GroupNode node, int index, String[] groupColumns, int[] groupColumnIndices,
                        Comparable[] groupValues, DataFrameHeader header,
                        DataFrame df, int rowIndex) {
        if (index == groupColumns.length) {
            if (!node.hasGroup()) {
                DataGroup group = new DataGroup(
                        groupColumns,
                        groupValues
                );
                group.set(header.copy());
                groups.add(group);
                node.setGroup(group);
            }
            node.addRow(df, rowIndex);
            return;
        }
        Comparable value = df.getValue(groupColumnIndices[index], rowIndex);
        groupValues[index] = value;
        GroupNode child;
        if ((child = node.getChild(value)) == null) {
            child = new GroupNode(value);
            node.addChild(child);
        }
        addRec(groups, child, index + 1, groupColumns, groupColumnIndices, groupValues, header, df, rowIndex);
    }

    private static DataFrameColumn[] createGroupColumns(DataFrame df, String... columns) {
        DataFrameColumn[] groupColumns = new DataFrameColumn[columns.length];
        for (int i = 0; i < columns.length; i++) {
            DataFrameColumn orgCol = df.getColumn(columns[i]);
            groupColumns[i] = orgCol.copyEmpty();
        }
        return groupColumns;
    }


    private class GroupNode {
        private Comparable value;
        private HashMap<Comparable, GroupNode> children;
        private DataGroup dataGroup;

        public GroupNode(Comparable value) {
            this.value = value;
        }

        public void clear() {
            if (children != null) {
                children.clear();
            }
            if (dataGroup != null) {
                dataGroup = null;
            }
        }

        private HashMap<Comparable, GroupNode> getChildrenMap() {
            if (children == null) {
                children = new HashMap<>();
            }
            return children;
        }

        public Comparable getValue() {
            return value;
        }

        public void addChild(GroupNode child) {
            getChildrenMap().put(child.getValue(), child);
        }

        public GroupNode getChild(Comparable value) {
            return getChildrenMap().get(value);
        }

        public void addRow(DataFrame df, int rowIndex) {
            dataGroup.append(df, rowIndex);
        }


        public void setGroup(DataGroup group) {
            this.dataGroup = group;
        }

        public boolean hasGroup() {
            return dataGroup != null;
        }

    }
}
