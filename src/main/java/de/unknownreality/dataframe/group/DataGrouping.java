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
    private DataGroup[] groups;
    private final String[] groupColumns;
    private final Map<MultiKey, DataGroup> groupMap = new HashMap<>();

    /**
     * Creates a data grouping based on a collections of {@link DataGroup data groups} and the corresponding group columns
     *
     * @param groups       data groups
     * @param groupColumns group columns
     */
    public DataGrouping(Collection<DataGroup> groups, String... groupColumns) {
        this.groupColumns = groupColumns;
        this.groups = new DataGroup[groups.size()];
        groups.toArray(this.groups);
        for (DataGroup g : groups) {
            groupMap.put(getKey((Comparable[]) g.getGroupValues().getValues()), g);
        }
    }

    public DataGroup getGroup(int index){
        return groups[index];
    }

    /**
     * Concatenates two data groupings. The data groups from the other data grouping are appended to this data grouping
     *
     * @param other other data grouping
     * @return <tt>self</tt> for method chaining
     */
    public DataGrouping concat(DataGrouping other) {
        if (!Arrays.equals(this.getGroupColumns(), other.getGroupColumns())) {
            throw new DataFrameRuntimeException("other DataGrouping must have the same GroupColumns");
        }
        DataGroup[] newGroups = new DataGroup[groups.length + other.size()];
        System.arraycopy(groups, 0, newGroups, 0, groups.length);
        int i = groups.length;
        for (DataGroup g : other.groups) {
            newGroups[i++] = g;
            groupMap.put(getKey((Comparable[]) g.getGroupValues().getValues()), g);
        }
        groups = newGroups;
        return this;
    }

    /**
     * Returns the group key for an array of values
     *
     * @param values input values
     * @return group key
     */
    private MultiKey getKey(Comparable... values) {

        return new MultiKey(values);
    }

    /**
     * Finds and returns a data group based on its group values
     *
     * @param values input group values
     * @return found data group. or <tt>null</tt> if no group was found
     */
    public DataGroup findByGroupValues(Comparable... values) {
        if (values.length != groupColumns.length) {
            throw new DataFrameRuntimeException("values must have same length as GroupColumns");
        }
        return groupMap.get(getKey(values));
    }

    /**
     * Columns used for this grouping
     *
     * @return group columns
     */
    public String[] getGroupColumns() {
        return groupColumns;
    }

    /**
     * Returns the number of data groups in this grouping
     *
     * @return number of data groups
     */
    public int size() {
        return groups.length;
    }


}
