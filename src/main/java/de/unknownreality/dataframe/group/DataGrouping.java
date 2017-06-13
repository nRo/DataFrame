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

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.common.MultiKey;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.sort.SortColumn;

import java.util.*;

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
        this.addIndex(GROUP_INDEX,groupColumns);
        this.groups = new DataGroup[groups.size()];
        groups.toArray(this.groups);
        for(DataFrameColumn col : groupColumns){
            addColumn(col);
        }
        for(int i = 0; i < groups.size();i++){
            this.groups[i] = groups.get(i);
            this.append(groups.get(i).getGroupValues().getValues());
        }
    }

    public DataGroup getGroup(int index){
        return groups[index];
    }

    public DataGroup getGroup(DataRow row){
        return getGroup(row.getIndex());
    }


    /**
     * Finds and returns a data group based on its group values
     *
     * @param values input group values
     * @return found data group. or <tt>null</tt> if no group was found
     */
    public GroupRow findByGroupValues(Comparable... values) {
       return (GroupRow)findFirstByIndex(GROUP_INDEX,values);
    }


    @Override
    public GroupRow getRow(int i) {
        return new GroupRow(getGroup(i),getHeader(), getRowValues(i),i);
    }
}
