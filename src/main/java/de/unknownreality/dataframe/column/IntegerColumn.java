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

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class IntegerColumn extends NumberColumn<Integer, IntegerColumn> {
    private static Logger log = LoggerFactory.getLogger(IntegerColumn.class);

    public IntegerColumn() {
        super();
    }

    public IntegerColumn(String name) {
        super(name);
    }

    public IntegerColumn(String name, Integer[] values) {
        super(name, values);
    }


    @Override
    protected IntegerColumn getThis() {
        return null;
    }

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }

    private final Parser<Integer> parser = ParserUtil.findParserOrNull(Integer.class);

    @Override
    public Parser<Integer> getParser() {
        return parser;
    }

    @Override
    public IntegerColumn copy() {
        Integer[] copyValues = new Integer[size()];
        toArray(copyValues);
        return new IntegerColumn(getName(), copyValues);
    }

}
