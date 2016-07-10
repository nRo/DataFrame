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

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Alex on 09.03.2016.
 */
public class DateColumn extends BasicColumn<Date, DateColumn> {
    private final DateFormat dateFormat;

    public DateColumn() {
        super();
        this.dateFormat = DateFormat.getDateInstance();
    }

    public DateColumn(DateFormat dateFormat) {
        super();
        this.dateFormat = dateFormat;
    }

    public DateColumn(DateFormat dateFormat, String name) {
        super(name);
        this.dateFormat = dateFormat;
    }

    public DateColumn(DateFormat dateFormat, String name, Date[] values) {
        super(name, values);
        this.dateFormat = dateFormat;
    }

    @Override
    protected DateColumn getThis() {
        return null;
    }

    @Override
    public Parser<Date> getParser() {
        return new Parser<Date>() {
            @Override
            public Date parse(String s) throws ParseException {
                return dateFormat.parse(s);
            }
        };
    }


    @Override
    public Class<Date> getType() {
        return Date.class;
    }


    @Override
    public DateColumn copy() {
        Date[] copyValues = new Date[size()];
        toArray(copyValues);
        return new DateColumn(dateFormat, getName(), copyValues);
    }
}
