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

package de.unknownreality.dataframe.group.impl;

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.group.DataGroup;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupUtil;
import de.unknownreality.dataframe.sort.SortColumn;

import java.util.ArrayList;
import java.util.Collection;
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
        Comparable[] groupValues = new Comparable[columns.length];
        for (DataRow row : df) {
            addRec(groupList,root,0,columns,groupValues,header,row);
        }
        root.clear();
        return new DataGrouping(groupList, createGroupColumns(df, columns));
    }

    private void addRec(List<DataGroup> groups,GroupNode node, int index,String[] groupColumns,Comparable[] groupValues,DataFrameHeader header, DataRow row) {
        if (index == groupColumns.length) {
            if(!node.hasGroup()){
                Comparable[] values = new Comparable[groupValues.length];
                System.arraycopy(groupValues,0,values,0,groupValues.length);
                DataGroup group = new DataGroup(
                        groupColumns,
                        values
                );
                group.set(header.copy(),new ArrayList<>(0));
                groups.add(group);
                node.setGroup(group);
            }
            node.addRow(row);
            return;
        }
        Comparable value = row.get(groupColumns[index]);
        groupValues[index] = value;
        GroupNode child;
        if ((child = node.getChild(value)) == null) {
            child = new GroupNode(value);
            node.addChild(child);
        }
        addRec(groups,child, index + 1,groupColumns,groupValues, header,row);
    }

    private static DataFrameColumn[] createGroupColumns(DataFrame df, String... columns){
        DataFrameColumn[] groupColumns = new DataFrameColumn[columns.length];
        for(int i = 0; i < columns.length; i++){
            DataFrameColumn orgCol = df.getColumn(columns[i]);
            groupColumns[i] = orgCol.copyEmpty();
        }
        return groupColumns;
    }


    private class GroupNode {
        private Comparable value;
        private HashMap<Comparable, GroupNode> children;
        private DataGroup dataGroup;

        public GroupNode( Comparable value) {
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

        public void addRow(DataRow row) {
            dataGroup.append(row);
        }


        public void setGroup(DataGroup group) {
            this.dataGroup = group;
        }

        public boolean hasGroup() {
            return dataGroup != null;
        }

    }
}
