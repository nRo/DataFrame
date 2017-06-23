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

package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.group.aggr.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 10.03.2016.
 */
public class DataGrouping extends DefaultDataFrame {
    public final static String GROUP_INDEX = "%group_index%";
    private DataGroup[] groups;

    /**
     * Creates a data grouping based on a collections of {@link DataGroup data groups} and the corresponding group columns
     *
     * @param groups       data groups
     * @param groupColumns group columns
     */
    public DataGrouping(List<DataGroup> groups, DataFrameColumn... groupColumns) {
        this.addIndex(GROUP_INDEX, groupColumns);
        this.groups = new DataGroup[groups.size()];
        groups.toArray(this.groups);
        for (DataFrameColumn col : groupColumns) {
            addColumn(col);
        }
        for (int i = 0; i < groups.size(); i++) {
            this.groups[i] = groups.get(i);
            this.append(groups.get(i).getGroupValues().getValues());
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> DataGrouping aggregate(String columnName, AggregateFunction<T> fun) {
        return agg(columnName,fun);
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> DataGrouping agg(String columnName, AggregateFunction<T> fun) {
        List<T> values = new ArrayList<>();
        for(int i = 0; i < size(); i++){
            T v = fun.aggregate(getRow(i).getGroup());
            values.add(v);
        }
        Class<? extends Comparable> vType = null;
        for(T v : values){
            if(v != null){
                vType = v.getClass();
                break;
            }
        }
        vType = vType == null ? String.class : vType;
        Class colType = ColumnTypeMap.get(vType);
        if(colType == null){
            throw new DataFrameRuntimeException(String.format("no column type found for value type '%s'", vType.getCanonicalName()));
        }
        DataFrameColumn<T,?> aggCol;
        try {
            aggCol = (DataFrameColumn<T,?>)colType.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
            throw new DataFrameRuntimeException(String.format("error creating instance of column [%s], empty constructor required", colType.getCanonicalName()), e);
        }
        aggCol.setName(columnName);
        for(T v : values){
            if(v == null){
                aggCol.appendNA();
                continue;
            }
            aggCol.append(v);
        }
        addColumn(aggCol);
        return this;
    }

    public DataGroup getGroup(int index) {
        return groups[index];
    }

    public DataGroup getGroup(DataRow row) {
        return getGroup(row.getIndex());
    }


    /**
     * Finds and returns a data group based on its group values
     *
     * @param values input group values
     * @return found data group. or <tt>null</tt> if no group was found
     */
    public GroupRow findByGroupValues(Comparable... values) {
        return (GroupRow) findFirstByIndex(GROUP_INDEX, values);
    }


    @Override
    public GroupRow getRow(int i) {
        return new GroupRow(getGroup(i), getHeader(), getRowValues(i), i);
    }

}
