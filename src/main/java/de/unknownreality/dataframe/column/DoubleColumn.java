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

package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;

/**
 * Created by Alex on 09.03.2016.
 */
public class DoubleColumn extends NumberColumn<Double, DoubleColumn> {

    public DoubleColumn() {
        super();
    }

    public DoubleColumn(String name) {
        super(name);
    }

    public DoubleColumn(String name, Double[] values) {
        super(name, values);
    }


    @Override
    public Class<Double> getType() {
        return Double.class;
    }

    private final Parser<Double> parser = ParserUtil.findParserOrNull(Double.class);

    @Override
    public Parser<Double> getParser() {
        return parser;
    }

    @Override
    protected DoubleColumn getThis() {
        return this;
    }


    @Override
    public DoubleColumn copy() {
        Double[] copyValues = new Double[size()];
        toArray(copyValues);
        return new DoubleColumn(getName(), copyValues);
    }
    @Override
    public <H> Double getValueFromRow(Row<?, H> row, H headerName) {
        return row.getDouble(headerName);
    }

    @Override
    public Double getValueFromRow(Row<?, ?> row, int headerIndex) {
        return row.getDouble(headerIndex);
    }
}
