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
import de.unknownreality.dataframe.type.impl.FloatType;

/**
 * Created by Alex on 09.03.2016.
 */
public class FloatColumn extends NumberColumn<Float, FloatColumn> {

    private final FloatType valueType = new FloatType(getSettings());

    @Override
    public FloatType getValueType() {
        return valueType;
    }

    public FloatColumn() {
        super(Float.class);
    }

    public FloatColumn(String name) {
        super(name, Float.class);
    }

    public FloatColumn(String name, Float[] values) {
        super(name, values);
    }

    public FloatColumn(String name, Float[] values, int size) {
        super(name, values, size);
    }


    @Override
    protected FloatColumn getThis() {
        return this;
    }


    @Override
    public FloatColumn copy() {
        Float[] copyValues = new Float[values.length];
        toArray(copyValues);
        return new FloatColumn(getName(), copyValues, size());
    }
    @Override
    public <H> Float getValueFromRow(Row<?, H> row, H headerName) {
        return row.getFloat(headerName);
    }

    @Override
    public Float getValueFromRow(Row<?, ?> row, int headerIndex) {
        return row.getFloat(headerIndex);
    }

    @Override
    public FloatColumn copyEmpty() {
        return new FloatColumn(getName());
    }
}
