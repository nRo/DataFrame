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

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.common.NumberUtil;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.type.ValueType;

import java.text.ParseException;

/**
 * Created by Alex on 09.03.2016.
 */
public class ComparePredicate extends FilterPredicate {
    /**
     * Operations available for compare predicates
     */
    public enum Operation {
        /**
         * greater than
         * <p><code>row column value &gt; comparison value</code></p>
         */
        GT(">"),

        /**
         * greater or equal
         * <p><code>row column value &gt;= comparison value</code></p>
         */
        GE(">="),

        /**
         * lower than
         * <p><code>row column value &lt; comparison value</code></p>
         */
        LT("<"),

        /**
         * lower or equal
         * <p><code>row column value &lt;= comparison value</code></p>
         */
        LE("<="),

        /**
         * equal
         * <p><code>row column value == comparison value</code></p>
         */
        EQ("=="),

        /**
         * not equal
         * <p><code>row column value != comparison value</code></p>
         */
        NE("!=");

        private final String str;

        Operation(String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    private final String headerName;
    private final Object value;
    private Object parsedValue;
    private final Operation operation;

    /**
     * Creates a compare predicate for a given row column name, operation
     * and a value the row value is compared with
     *
     * @param headerName row column name
     * @param op         compare operation
     * @param value      value for comparison
     */
    public ComparePredicate(String headerName, Operation op, Object value) {
        this.headerName = headerName;
        this.operation = op;
        this.value = value;
    }

    public String getHeaderName() {
        return headerName;
    }

    public Operation getOperation() {
        return operation;
    }

    public Object getValue() {
        return value;
    }

    /**
     * Returns <tt>true</tt> if the row is valid for this predicate
     *
     * @param row tested row
     * @return <tt>true</tt> if the row is valid
     */
    @Override
    public boolean valid(Row<?, String> row) {
        ValueType<?> type = row.getType(headerName);
        Object v;
        try {
            v = convertValue(type);
        } catch (ParseException e) {
            throw new DataFrameFilterRuntimeException(
                    String.format("error converting filter value '%s' to '%s' in column '%s'",
                            value, type.getType().getCanonicalName(), headerName));
        }
        return compare(type, row.get(headerName), v);

    }


    @SuppressWarnings("unchecked")
    protected <T> T convertValue(ValueType<T> type) throws ParseException {
        if (value == null) {
            return null;
        }
        if (type.getType().isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        if (this.parsedValue != null && type.getType().isAssignableFrom(parsedValue.getClass())) {
            return (T) parsedValue;
        }
        if (Number.class.isAssignableFrom(type.getType())) {
            Class<? extends Number> cl = (Class<? extends Number>) type.getType();
            if (Number.class.isAssignableFrom(value.getClass())) {
                parsedValue = NumberUtil.convert((Number) value, cl);
                return (T) parsedValue;
            }
        }
        if (Values.NA.isNA(value)) {
            parsedValue = null;
            return null;
        }
        parsedValue = type.parse(String.valueOf(value));
        return (T) parsedValue;
    }

    protected <T> boolean compare(ValueType<?> type, Object rowValue, Object predicateValue) {

        boolean isValueRowValueNA = Values.NA.isNA(rowValue);
        boolean isPredicateNA = Values.NA.isNA(predicateValue);
        if (isValueRowValueNA && isPredicateNA) {
            return isValid(operation, 0);
        }
        if (isPredicateNA || isValueRowValueNA) {
            return operation == Operation.NE;
        }
        if (operation == Operation.EQ && rowValue.equals(predicateValue)) {
            return true;
        }
        int c = type.compareRaw(rowValue, predicateValue);
        return isValid(operation, c);
    }


    protected boolean isValid(Operation operation, int c) {
        switch (operation) {
            case GT:
                return c > 0;
            case GE:
                return c >= 0;
            case LT:
                return c < 0;
            case LE:
                return c <= 0;
            case EQ:
                return c == 0;
            case NE:
                return c != 0;
            default:
                throw new DataFrameRuntimeException(String.format("unknown operation: %s", operation.str));
        }
    }

    @Override
    public String toString() {
        return headerName + " " + operation + " " + value;
    }
}
