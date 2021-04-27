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

package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.type.impl.LongType;

/**
 * Created by Alex on 09.03.2016.
 */
public class LongColumn extends NumberColumn<Long, LongColumn> {

    private final LongType valueType = new LongType(getSettings());

    public LongColumn() {
        super(Long.class);
    }

    public LongColumn(String name) {
        super(name, Long.class);
    }

    public LongColumn(String name, Long[] values) {
        super(name, values);
    }

    public LongColumn(String name, Long[] values, int size) {
        super(name, values, size);
    }


    @Override
    protected LongColumn getThis() {
        return this;
    }

    @Override
    public LongType getValueType() {
        return valueType;
    }

    @Override
    public LongColumn copy() {
        Long[] copyValues = new Long[values.length];
        toArray(copyValues);
        return new LongColumn(getName(), copyValues, size());
    }

    @Override
    public <H> Long getValueFromRow(Row<?, H> row, H headerName) {
        return row.getLong(headerName);
    }

    @Override
    public Long getValueFromRow(Row<?, ?> row, int headerIndex) {
        return row.getLong(headerIndex);
    }

    @Override
    public LongColumn copyEmpty() {
        return new LongColumn(getName());
    }
}
