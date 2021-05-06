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

package de.unknownreality.dataframe.group.aggr;

import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.filter.FilterPredicate;

/**
 * Created by Alex on 13.06.2017.
 */
public class Aggregate {
    public static final AggregateFunction<Integer> count = (DefaultDataFrame::size);

    public static AggregateFunction<Integer> count() {
        return count;
    }

    public static AggregateFunction<Double> mean(final String colName) {
        return group -> group.getNumberColumn(colName).mean();
    }

    public static AggregateFunction<Number> median(final String colName) {
        return group -> group.getNumberColumn(colName).median();
    }

    public static AggregateFunction<Number> min(final String colName) {
        return group -> group.getNumberColumn(colName).min();
    }


    public static AggregateFunction<Number> max(final String colName) {
        return group -> group.getNumberColumn(colName).max();
    }

    public static AggregateFunction<Integer> filterCount(FilterPredicate filterPredicate) {
        return group -> group.selectRows(filterPredicate).size();
    }

    public static AggregateFunction<Integer> filterCount(String predicateString) {
        return group -> group.selectRows(predicateString).size();
    }

    public static AggregateFunction<Object> first(final String colName) {
        return group -> group.getRow(0).get(colName);
    }

    public static AggregateFunction<Object> last(final String colName) {
        return group -> group.getRow(group.size() - 1).get(colName);
    }


    public static AggregateFunction<Number> quantile(final String colName, double quantile) {
        return group -> group.getNumberColumn(colName).getQuantile(quantile);
    }


    public static AggregateFunction<Integer> naCount(String column) {
        return group -> {
            int c = 0;
            for (int i = 0; i < group.size(); i++) {
                if (group.getRow(i).isNA(column)) {
                    c++;
                }
            }
            return c;
        };
    }

    public static AggregateFunction<String> join(String delimiter, String column) {
        return group -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < group.size(); i++) {
                if (i > 0) {
                    sb.append(delimiter);
                }
                if (!group.getRow(i).isNA(column)) {
                    sb.append(group.getRow(i).toString(column));
                }
            }
            return sb.toString();
        };
    }

}
