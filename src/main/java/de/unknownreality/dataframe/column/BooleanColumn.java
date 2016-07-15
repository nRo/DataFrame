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


import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.MapFunction;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;

import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public class BooleanColumn extends BasicColumn<Boolean, BooleanColumn> {

    public BooleanColumn and(BooleanColumn other) {
        for(int i  = 0; i < Math.min(size(),other.size());i++){
            values[i] = values[i] && other.values[i];
        }
        return this;
    }

    public BooleanColumn andNot(BooleanColumn other) {
        for(int i  = 0; i < Math.min(size(),other.size());i++){
            values[i] = values[i] && !other.values[i];
        }
        return this;
    }

    public BooleanColumn or(BooleanColumn other) {
        for(int i  = 0; i < Math.min(size(),other.size());i++){
            values[i] = values[i] || other.values[i];
        }
        return this;
    }

    public BooleanColumn xor(BooleanColumn other) {
        for(int i  = 0; i < Math.min(size(),other.size());i++){
            values[i] = values[i] != other.values[i];
        }
        return this;
    }

    public BooleanColumn flip() {
        for(int i  = 0; i < size();i++){
            values[i] = !values[i];
        }
        return this;
    }

    public BooleanColumn() {
        super();
    }

    public BooleanColumn(String name) {
        super(name);
    }

    public BooleanColumn(String name, Boolean[] values) {
        super(name, values);
    }


    @Override
    protected BooleanColumn getThis() {
        return null;
    }

    @Override
    public Class<Boolean> getType() {
        return Boolean.class;
    }

    private final Parser<Boolean> parser = ParserUtil.findParserOrNull(Boolean.class);

    @Override
    public Parser<Boolean> getParser() {
        return parser;
    }

    @Override
    public BooleanColumn copy() {
        Boolean[] copyValues = new Boolean[size()];
        toArray(copyValues);
        return new BooleanColumn(getName(), copyValues);
    }


}
