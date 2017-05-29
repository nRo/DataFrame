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

import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.sort.SortColumn;

import java.util.Comparator;

/**
 * Created by Alex on 09.03.2016.
 */
public class GroupValueComparator implements Comparator<DataGroup> {
    private final SortColumn[] sortColumns;

    /**
     * Creates a comparator for {@link DataGroup data groups} using specified sort columns
     * Sort column specify the group values used for sorting
     *
     * @param sortColumns sort columns
     */
    public GroupValueComparator(SortColumn[] sortColumns) {
        this.sortColumns = sortColumns;
    }

    /**
     * compares two data groups using the specified sort columns.
     *
     * @param r1 first data group
     * @param r2 second data group
     * @return comparison result
     */
    @SuppressWarnings("unchecked")
    @Override
    public int compare(DataGroup r1, DataGroup r2) {
        int c = 0;
        for (SortColumn sortColumn : sortColumns) {
            Comparable r1Val = r1.getGroupValues().get(sortColumn.getName());
            Comparable r2Val = r2.getGroupValues().get(sortColumn.getName());
            boolean r1NA = r1Val == null || r1Val == Values.NA;
            boolean r2NA = r2Val == null || r2Val == Values.NA;

            if (r1NA && r2NA) {
                c = 0;
                continue;
            }
            if (r1NA) {
                return 1;
            }
            if (r2NA) {
                return -1;
            }
            c = r1Val.compareTo(r2Val);
            c = sortColumn.getDirection() == SortColumn.Direction.Ascending ? c : -c;
            if (c != 0) {
                return c;
            }
        }
        return c;
    }
}
