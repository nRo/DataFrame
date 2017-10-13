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

package de.unknownreality.dataframe.filter;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.NumberUtil;
import de.unknownreality.dataframe.common.Row;

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
    @SuppressWarnings("unchecked")
    @Override
    public boolean valid(Row row) {
        return compare(row.get(headerName),value);
    }

    protected boolean compare(Object valueA, Object valueB){
        if (operation == Operation.EQ && valueA.equals(valueB)) {
            return true;
        }
        if(valueA instanceof String && valueB instanceof Number){
            Number n ;
            if((n = NumberUtil.parseNumberOrNull(valueA.toString())) != null){
                valueA = n;
            }
            else{
                valueB = NumberUtil.toString((Number)valueB);
            }
        }

        boolean numberCompare = (valueA instanceof Number && valueB instanceof Number);
        if (!valueA.getClass().equals(valueB.getClass()) && !numberCompare) {
            return operation == Operation.NE;
        }
        int c = 0;
        if (numberCompare) {
            c = NumberUtil.compare((Number) valueA, (Number) valueB);
        } else if (valueA instanceof Comparable && valueB instanceof Comparable) {
            c = ((Comparable) valueA).compareTo(valueB);
        }
        return isValid(operation,c);
    }


    protected boolean isValid (Operation operation,int c){
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
                throw new DataFrameRuntimeException(String.format("unknown operation: %s",operation.str));
        }
    }

    @Override
    public String toString() {
        return headerName + " " + operation + " " + value;
    }
}
