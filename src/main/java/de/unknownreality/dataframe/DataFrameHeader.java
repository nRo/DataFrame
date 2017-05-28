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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.header.BasicTypeHeader;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameHeader extends BasicTypeHeader<String>{



    /**
     * Adds a new data frame column to this header
     * @param column new data frame column
     * @return <tt>self</tt> for method chaining
     */
    public DataFrameHeader add(DataFrameColumn<?, ?> column) {
        return (DataFrameHeader)add(column.getName(), column.getClass(), column.getType());
    }

    @Override
    public DataFrameHeader copy() {
        DataFrameHeader copy = new DataFrameHeader();
        for (String h : this) {
            copy.add(h, getColumnType(h), getType(h));
        }
        return copy;
    }
}
