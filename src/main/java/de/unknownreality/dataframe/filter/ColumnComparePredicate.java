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

package de.unknownreality.dataframe.filter;

import de.unknownreality.dataframe.common.NumberUtil;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.type.ValueType;

import java.text.ParseException;

/**
 * Created by Alex on 07.06.2017.
 */
public class ColumnComparePredicate extends ComparePredicate {

    private String headerB;

    /**
     * Creates a compare predicate for two given row column names and operation
     *
     * @param headerA first column name
     * @param op      compare operation
     * @param headerB second column name
     */
    public ColumnComparePredicate(String headerA, Operation op, String headerB) {
        super(headerA, op, null);
        this.headerB = headerB;
    }

    /**
     * Returns <tt>true</tt> if the row is valid for this predicate
     *
     * @param row tested row
     * @return <tt>true</tt> if the row is valid
     */
    @Override
    public boolean valid(Row<?, String> row) {
        ValueType<?> type = row.getType(getHeaderName());
        ValueType<?> typeB = row.getType(headerB);
        if (type.getType().isAssignableFrom(typeB.getType())) {
            return compare(type, row.get(getHeaderName()), row.get(headerB));
        }
        Object vB = row.get(headerB);
        Object convertedB;
        try {
            convertedB = convertValue(type, vB);
        } catch (ParseException e) {
            throw new DataFrameFilterRuntimeException(
                    String.format("error converting filter value '%s' to '%s' in column '%s'",
                            vB, type.getType().getCanonicalName(), headerB));
        }
        return super.compare(type, row.get(getHeaderName()), convertedB);
    }

    @SuppressWarnings("unchecked")
    protected <T> T convertValue(ValueType<T> type, Object value) throws ParseException {
        if (value == null) {
            return null;
        }
        if (type.getType().isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        if (Number.class.isAssignableFrom(type.getType())) {
            Class<? extends Number> cl = (Class<? extends Number>) type.getType();
            if (Number.class.isAssignableFrom(value.getClass())) {
                return (T) NumberUtil.convert((Number) value, cl);
            }
        }
        return type.parse(String.valueOf(value));
    }

    @Override
    public String toString() {
        return "." + getHeaderName() + " " + getOperation() + " ." + headerB;
    }
}
