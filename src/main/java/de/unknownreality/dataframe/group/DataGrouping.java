/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.MultiKey;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.sort.SortColumn;

import java.util.*;

/**
 * Created by Alex on 10.03.2016.
 */
public class DataGrouping implements Iterable<DataGroup> {
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
        for (DataGroup g : other) {
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

    /**
     * Finds data groups using a {@link FilterPredicate}.
     *
     * @param predicate input predicate
     * @return list of found data groups
     */
    private List<DataGroup> findGroups(FilterPredicate predicate) {
        List<DataGroup> groups = new ArrayList<>();
        for (DataGroup g : this) {
            if (predicate.valid(g.getGroupValues())) {
                groups.add(g);
            }
        }
        return groups;
    }

    /**
     * Filters data groups that are not valid according to an input predicate.<br>
     * Data groups are filtered by their group values. <br>
     * The predicated treats group values like data rows. <br>
     * If a data group is <b>filtered</b> if it is <b>not valid</b> according to the predicate.<br>
     * The filtered data groups are removed from this data grouping.<br>
     * <p><code>if(!predicate.valid(dataGroup)) -&gt; remove(dataGroup)</code></p>
     *
     * @param predicate filter predicate
     * @return <tt>self</tt> for method chaining
     */
    public DataGrouping filter(FilterPredicate predicate) {
        List<DataGroup> groups = findGroups(predicate);
        this.groups = new DataGroup[groups.size()];
        groups.toArray(this.groups);
        return this;
    }

    /**
     * Returns a new data grouping based on filtered data groups from this data grouping.<br>
     * Data groups that are valid according to the input predicate remain in the new data grouping.<br>
     * <p><code>if(predicate.valid(dataGroup)) -&gt; add(dataGroup)</code></p>
     *
     * @param predicate filter predicate
     * @return new data grouping including the found data groups
     * @see #filter(FilterPredicate)
     */
    public DataGrouping find(FilterPredicate predicate) {
        List<DataGroup> groups = findGroups(predicate);
        return new DataGrouping(groups, groupColumns);
    }

    /**
     * Returns a new data grouping with all data groups from this grouping where a specified group value equals
     * an input value.
     *
     * @param colName group value column name
     * @param value   input value
     * @return new data grouping including the found data groups
     */
    public DataGrouping find(String colName, Comparable value) {
        return find(FilterPredicate.eq(colName, value));
    }

    /**
     * Returns the first found data group from this grouping where a specified group value equals
     * an input value.
     *
     * @param colName group value column name
     * @param value   input value
     * @return first found data group
     */
    public DataGroup findFirst(String colName, Comparable value) {
        return findFirst(FilterPredicate.eq(colName, value));

    }

    /**
     * Returns the first found data group from this data grouping matching an input predicate.
     *
     * @param predicate input predicate
     * @return first found data group
     * @see #find(FilterPredicate)
     */
    public DataGroup findFirst(FilterPredicate predicate) {
        for (DataGroup row : this) {
            if (predicate.valid(row.getGroupValues())) {
                return row;
            }
        }
        return null;
    }

    /**
     * Sorts the data groups in this data grouping using one or more {@link SortColumn}.
     * Sort column specify the group values used for sorting
     *
     * @param columns sort columns
     * @return <tt>self</tt> for method chaining
     * @see SortColumn
     */
    public DataGrouping sort(SortColumn... columns) {
        Arrays.sort(groups, new GroupValueComparator(columns));
        return this;
    }

    /**
     * Sorts the data groups using a specified group value
     *
     * @param name group value name used for sorting
     * @return <tt>self</tt> for method chaining
     * @see #sort(SortColumn...)
     */
    public DataGrouping sort(String name) {
        return sort(name, SortColumn.Direction.Ascending);
    }

    /**
     * Sorts the data groups using a specified group value and a sort direction
     *
     * @param name group value name used for sorting
     * @param dir  sort direction (asc|desc)
     * @return <tt>self</tt> for method chaining
     * @see #sort(SortColumn...)
     */
    public DataGrouping sort(String name, SortColumn.Direction dir) {
        Arrays.sort(groups, new GroupValueComparator(new SortColumn[]{new SortColumn(name, dir)}));
        return this;
    }

    /**
     * Sorts the data groups using a custom comparator.
     *
     * @param comp comparator used to sort data groups
     * @return <tt>self</tt> for method chaining
     */
    public DataGrouping sort(Comparator<DataGroup> comp) {
        Arrays.sort(groups, comp);
        return this;
    }


    /**
     * Returns an iterator over the data groups in this grouping.
     * <p>{@link Iterator#remove()} is not supported</p>
     *
     * @return data groups iterator
     */
    @Override
    public Iterator<DataGroup> iterator() {
        return new Iterator<DataGroup>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < groups.length;
            }

            @Override
            public DataGroup next() {
                return groups[index++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported in data groupings");
            }
        };
    }
}
