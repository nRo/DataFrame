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

package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;

/**
 * Created by Alex on 09.03.2016.
 */
public class LongColumn extends NumberColumn<Long, LongColumn> {

    private final Parser<Long> parser = ParserUtil.findParserOrNull(Long.class);

    public LongColumn() {
        super();
    }

    public LongColumn(String name) {
        super(name);
    }

    public LongColumn(String name, Long[] values) {
        super(name, values);
    }


    @Override
    protected LongColumn getThis() {
        return this;
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }


    @Override
    public Parser<Long> getParser() {
        return parser;
    }


    @Override
    public LongColumn copy() {
        Long[] copyValues = new Long[size()];
        toArray(copyValues);
        return new LongColumn(getName(), copyValues);
    }
    @Override
    public <H> Long getValueFromRow(Row<?, H> row, H headerName) {
        return row.getLong(headerName);
    }

    @Override
    public Long getValueFromRow(Row<?, ?> row, int headerIndex) {
        return row.getLong(headerIndex);
    }
}
