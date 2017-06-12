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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.index.Indices;

import java.util.Collection;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DefaultDataFrame extends AbstractDataFrame<DefaultDataFrameHeader,DataRow,DefaultDataFrame> {
    private DefaultDataFrameHeader header = new DefaultDataFrameHeader();
    private final Indices indices = new Indices(this);

    public DefaultDataFrame() {

    }

    /**
     * Creates a new data frame using a data frame header and a collections of data rows
     *
     * @param header data frame header
     * @param rows   collections of data rows
     */
    public DefaultDataFrame(DefaultDataFrameHeader header, Collection<DataRow> rows) {
        set(header, rows);
    }

    /**
     * Returns the data row at a specified index
     *
     * @param i index of data row
     * @return data row at  specified index
     */
    @Override
    public DataRow getRow(int i) {
        return new DataRow(header, getRowValues(i), i);
    }

    @Override
    public DefaultDataFrame getThis() {
        return this;
    }

    @Override
    protected void setHeader(DefaultDataFrameHeader header) {
        this.header = header;
    }

    @Override
    public DefaultDataFrame createNew(DefaultDataFrameHeader header, List<DataRow> rows, Indices indices) {
        DefaultDataFrame basicDataFrame = new DefaultDataFrame();
        basicDataFrame.set(header,rows,indices);
        return basicDataFrame;
    }

    @Override
    public DefaultDataFrameHeader getHeader() {
        return header;
    }

    public DefaultDataFrame copy() {
        List<DataRow> rows = getRows(0, getSize());
        DefaultDataFrame copy = new DefaultDataFrame();
        copy.set(header.copy(), rows, indices);
        return copy;
    }

}
