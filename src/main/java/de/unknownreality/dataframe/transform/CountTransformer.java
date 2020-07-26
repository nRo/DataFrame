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

package de.unknownreality.dataframe.transform;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.column.IntegerColumn;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by algru on 06.09.2016.
 */
public class CountTransformer<T> implements ColumnDataFrameTransform<DataFrameColumn<T, ?>> {
    public static final String COUNTS_COLUMN = "counts";
    private final boolean ignoreNA;

    public CountTransformer(boolean ignoreNA) {
        this.ignoreNA = ignoreNA;
    }

    public CountTransformer() {
        this(true);
    }

    /**
     * Creates a new dataframe containing the values of the input column and a column with the corresponding counts
     *
     * @param source input column
     * @return count dataframe
     */
    @Override
    public DataFrame transform(DataFrameColumn<T, ?> source) {
        DataFrame countDataFrame = new DefaultDataFrame();
        DataFrameColumn<T, ?> valueColumn = source.copyEmpty();
        valueColumn.setName(source.getName());
        IntegerColumn countColumn = new IntegerColumn(COUNTS_COLUMN);

        Map<T, Integer> counts = new LinkedHashMap<>();
        for (int i = 0; i < source.size(); i++) {
            if (ignoreNA && source.isNA(i)) {
                continue;
            }
            T v = source.get(i);
            Integer count = counts.get(v);
            count = count == null ? 0 : count;
            count++;
            counts.put(v, count);
        }
        for (Map.Entry<T, Integer> entry : counts.entrySet()) {
            valueColumn.append(entry.getKey());
            countColumn.append(entry.getValue());
        }
        countDataFrame.addColumn(valueColumn);
        countDataFrame.addColumn(countColumn);
        return countDataFrame;
    }
}
