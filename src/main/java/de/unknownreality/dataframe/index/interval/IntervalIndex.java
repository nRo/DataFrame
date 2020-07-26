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

package de.unknownreality.dataframe.index.interval;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.column.NumberColumn;
import de.unknownreality.dataframe.index.Index;

import java.util.*;

public class IntervalIndex implements Index {
    private final Map<Integer, Interval> intervalMap = new HashMap<>();
    private final IntervalSearchTree<Integer> intervalSearchTree = new IntervalSearchTree<>();
    private NumberColumn<?, ?> startColumn;
    private NumberColumn<?, ?> endColumn;
    private final String name;

    public static IntervalIndex create(DataFrame dataFrame, String name, String startColumn, String endColumn) {
        return new IntervalIndex(name, dataFrame.getNumberColumn(startColumn), dataFrame.getNumberColumn(endColumn));
    }

    public static IntervalIndex create(String name, NumberColumn<?, ?> startColumn, NumberColumn<?, ?> endColumn) {
        return new IntervalIndex(name, startColumn, endColumn);
    }

    public IntervalIndex(String name, NumberColumn<?, ?> startColumn, NumberColumn<?, ?> endColumn) {
        this.name = name;
        this.startColumn = startColumn;
        this.endColumn = endColumn;
    }

    @Override
    public void update(DataRow dataRow) {
        if (intervalMap.containsKey(dataRow.getIndex())) {
            Interval interval = intervalMap.get(dataRow.getIndex());
            intervalSearchTree.remove(interval);
        }
        Interval interval = createInterval(dataRow);
        intervalMap.put(dataRow.getIndex(), interval);
        intervalSearchTree.add(interval, dataRow.getIndex());
    }

    @Override
    public void remove(DataRow dataRow) {
        if (intervalMap.containsKey(dataRow.getIndex())) {
            Interval interval = intervalMap.get(dataRow.getIndex());
            intervalSearchTree.remove(interval);
        }
    }

    private Interval createInterval(DataRow row) {
        return new Interval(
                row.getNumber(startColumn.getName()),
                row.getNumber(endColumn.getName()));
    }

    @Override
    public Collection<Integer> find(Object... values) {
        if (values.length == 1) {
            if (!(values[0] instanceof Number)) {
                throw new DataFrameRuntimeException("stab value must be a number for interval search");
            }
            return intervalSearchTree.stab((Number) values[0]);

        }
        if (values.length != 2) {
            throw new DataFrameRuntimeException("start and end values are required for interval search");
        }
        if(!(values[0] instanceof Number) || !(values[1] instanceof Number)){
            throw new DataFrameRuntimeException("start and end values must be numbers for interval search");

        }
        return intervalSearchTree.searchAll((Number)values[0],(Number)values[1]);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setUnique(boolean unique) {
        throw new DataFrameRuntimeException("unique is not supported by interval indices");
    }

    @Override
    public boolean containsColumn(DataFrameColumn<?, ?> column) {
        return startColumn == column || endColumn == column;
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public List<DataFrameColumn<?, ?>> getColumns() {
        List<DataFrameColumn<?, ?>> columns = new ArrayList<>();
        columns.add(startColumn);
        columns.add(endColumn);
        return columns;
    }

    @Override
    public void clear() {
        intervalMap.clear();
        intervalSearchTree.clear();
    }

    @Override
    public void replaceColumn(DataFrameColumn<?, ?> existing, DataFrameColumn<?, ?> replacement) {
        if (!(replacement instanceof NumberColumn<?, ?>)) {
            throw new DataFrameRuntimeException("only number columns are supported by interval indices");
        }
        clear();
        if (startColumn == existing) {
            startColumn = (NumberColumn<?, ?>) replacement;
        }
        if (endColumn == existing) {
            endColumn = (NumberColumn<?, ?>) replacement;
        }
        for (int i = 0; i < startColumn.size(); i++) {
            Interval interval = new Interval(startColumn.get(i),endColumn.get(i));
            intervalMap.put(i,interval);
            intervalSearchTree.add(interval,i);
        }
    }
}
