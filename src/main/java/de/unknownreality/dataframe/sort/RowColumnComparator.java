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

package de.unknownreality.dataframe.sort;

import de.unknownreality.dataframe.DataRow;

import java.util.Comparator;

/**
 * Created by Alex on 09.03.2016.
 */
public class RowColumnComparator implements Comparator<DataRow> {
    private final SortColumn[] sortColumns;

    /**
     * Creates a comparator using sort columns
     *
     * @param sortColumns sort columns
     */
    public RowColumnComparator(SortColumn[] sortColumns) {
        this.sortColumns = sortColumns;

    }

    /**
     * Compares two rows using the sort columns
     *
     * @param r1 first row
     * @param r2 second row
     * @return comparison result
     */
    @SuppressWarnings("unchecked")
    @Override
    public int compare(DataRow r1, DataRow r2) {
        int c = 0;
        for (int i = 0; i <sortColumns.length;i++) {
            SortColumn sortColumn = sortColumns[i];
            String name = sortColumn.getName();
            if (r1.isNA(name) && r2.isNA(name)) {
                c = 0;
                continue;
            }
            if (r1.isNA(name)) {
                return 1;
            }
            if (r2.isNA(name)) {
                return -1;
            }
            Comparable a = r1.get(sortColumn.getName());
            Comparable b = r2.get(sortColumn.getName());
            c = a.compareTo(b);
            c = sortColumn.getDirection() == SortColumn.Direction.Ascending ? c : -c;
            if (c != 0) {
                return c;
            }
        }
        return c;
    }
}
