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

package de.unknownreality.dataframe.filter;

import de.unknownreality.dataframe.common.Row;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class FilterPredicate {

    public static FilterPredicate EMPTY = empty();

    /**
     * Returns <tt>true</tt> if the row is valid for this predicate
     *
     * @param row tested row
     * @return <tt>true</tt> if the row is valid
     */
    public abstract boolean valid(Row row);

    /**
     * Returns a string representation for this predicate
     *
     * @return string representation
     */

    public abstract String toString();

    /**
     * Creates an <tt>empty</tt> predicate. Always returns <tt>true</tt>.
     * @return empty predicate
     */
    public static FilterPredicate empty(){
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return true;
            }

            @Override
            public String toString() {
                return "";
            }
        };
    }

    /**
     * Creates a new <tt>AND</tt> predicate using this predicate and an input predicate
     *
     * @param p input predicate
     * @return <tt>AND</tt> predicate
     * @see #and(FilterPredicate, FilterPredicate)
     */
    public FilterPredicate and(FilterPredicate p) {
        return FilterPredicate.and(this, p);
    }


    /**
     * Creates a new <tt>OR</tt> predicate using this predicate and an input predicate
     *
     * @param p input predicate
     * @return <tt>OR</tt> predicate
     * @see #or(FilterPredicate, FilterPredicate)
     */
    public FilterPredicate or(FilterPredicate p) {
        return FilterPredicate.or(this, p);
    }

    /**
     * Creates a new <tt>XOR</tt> predicate using this predicate and an input predicate
     *
     * @param p input predicate
     * @return <tt>XOR</tt> predicate
     * @see #xor(FilterPredicate, FilterPredicate)
     */
    public FilterPredicate xor(FilterPredicate p) {
        return FilterPredicate.xor(this, p);
    }

    /**
     * Negates this predicate
     *
     * @return negated predicate
     */
    public FilterPredicate neg() {
        return FilterPredicate.not(this);
    }


    /**
     * Returns the not predicate for a specified input predicate.
     * <code>return !filterPredicate.valid(row)</code>
     *
     * @param filterPredicate input predicate
     * @return Returns the negates result of the input predicate
     */
    public static FilterPredicate not(final FilterPredicate filterPredicate) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return !filterPredicate.valid(row);
            }

            @Override
            public String toString() {
                return "!(" + filterPredicate.toString() + ")";
            }
        };
    }


    /**
     * Compares two predicates and returns <tt>true</tt> if the results are not equal
     * <code>return p1.valid(row) != p2.valid(row)</code>
     *
     * @param p1 first input predicate
     * @param p2 second input predicate
     * @return <tt>true</tt> if one input predicates returns <tt>true</tt> and the other returns <tt>false</tt>
     */
    public static FilterPredicate ne(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row value) {
                return p1.valid(value) != p2.valid(value);
            }

            @Override
            public String toString() {
                return "(" + p1.toString() + ") != (" + p2.toString() + ")";
            }
        };
    }

    /**
     * Compares two predicates and returns <tt>true</tt> if the results are equal
     * <code>return p1.valid(row) == p2.valid(row)</code>
     *
     * @param p1 first input predicate
     * @param p2 second input predicate
     * @return <tt>true</tt> if both input predicates return either <tt>true</tt> or <tt>false</tt>
     */
    public static FilterPredicate eq(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) == p2.valid(row);
            }

            @Override
            public String toString() {
                return "(" + p1.toString() + ") == (" + p2.toString() + ")";
            }
        };
    }


    /**
     * Returns <tt>true</tt> if all input predicates return <tt>true</tt>.
     * <code>return p1.valid(row) AND p2.valid(row) AND ... AND pn.valid(row)</code>
     *
     * @param predicates input predicates
     * @return <tt>true</tt> if all input predicates return  <tt>true</tt>
     */
    public static FilterPredicate and(final FilterPredicate... predicates) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                for (FilterPredicate predicate : predicates) {
                    if (!predicate.valid(row)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < predicates.length; i++) {
                    sb.append("(").append(predicates[i]).append(")");
                    if (i < predicates.length - 1) {
                        sb.append(" AND ");
                    }
                }
                return sb.toString();
            }
        };
    }

    /**
     * Returns <tt>true</tt> if at least one input predicate returns <tt>true</tt>.
     * <code>return p1.valid(row) OR p2.valid(row) OR ... OR pn.valid(row)</code>
     *
     * @param predicates input predicates
     * @return <tt>true</tt> if at least one input predicate returns <tt>true</tt>
     */
    public static FilterPredicate or(final FilterPredicate... predicates) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                for (FilterPredicate predicate : predicates) {
                    if (predicate.valid(row)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < predicates.length; i++) {
                    sb.append("(").append(predicates[i]).append(")");
                    if (i < predicates.length - 1) {
                        sb.append(" OR ");
                    }
                }
                return sb.toString();
            }
        };
    }


    /**
     * Returns <tt>true</tt> if both input predicates return <tt>true</tt>.
     * <code>return p1.valid(row) AND p2.valid(row)</code>
     *
     * @param p1 first input predicate
     * @param p2 second input predicate
     * @return <tt>true</tt> if both input predicates return  <tt>true</tt>
     */
    public static FilterPredicate and(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) && p2.valid(row);
            }

            @Override
            public String toString() {
                return "(" + p1.toString() + ") AND (" + p2.toString() + ")";
            }
        };
    }

    /**
     * Returns <tt>true</tt> if at least one input predicate returns <tt>true</tt>.
     * <code>return p1.valid(row) OR p2.valid(row)</code>
     *
     * @param p1 first input predicate
     * @param p2 second input predicate
     * @return <tt>true</tt> if at least one input predicate returns <tt>true</tt>
     */
    public static FilterPredicate or(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) || p2.valid(row);
            }

            @Override
            public String toString() {
                return "(" + p1.toString() + ") OR (" + p2.toString() + ")";
            }
        };
    }

    /**
     * Returns <tt>true</tt> if one input predicate returns <tt>true</tt> and the other predicate returns <tt>false</tt>.
     * <code>return p1.valid(row) OR p2.valid(row)</code>
     *
     * @param p1 first input predicate
     * @param p2 second input predicate
     * @return <tt>true</tt> if one input predicate returns <tt>true</tt> and the other returns <tt>false</tt>
     */
    public static FilterPredicate xor(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                boolean p1v = p1.valid(row);
                boolean p2v = p2.valid(row);
                return (p1v && !p2v) || (p2v && !p1v);
            }

            @Override
            public String toString() {
                return "(" + p1.toString() + ") XOR (" + p2.toString() + ")";
            }
        };
    }

    /**
     * Creates a {@link ComparePredicate} using {@link de.unknownreality.dataframe.filter.ComparePredicate.Operation#NE not equals operation}
     *
     * @param name  row column name
     * @param value value for comparison
     * @return <tt>'not equals'</tt> predicate
     * @see de.unknownreality.dataframe.filter.ComparePredicate.Operation#NE
     */
    public static FilterPredicate ne(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.NE, value);
    }

    /**
     * Creates a {@link ComparePredicate} using {@link de.unknownreality.dataframe.filter.ComparePredicate.Operation#EQ equals operation}
     *
     * @param name  row column name
     * @param value value for comparison
     * @return <tt>'equals'</tt> predicate
     * @see de.unknownreality.dataframe.filter.ComparePredicate.Operation#EQ
     */
    public static FilterPredicate eq(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.EQ, value);
    }

    /**
     * Creates a {@link ComparePredicate} using {@link de.unknownreality.dataframe.filter.ComparePredicate.Operation#GT greater than operation}
     *
     * @param name  row column name
     * @param value value for comparison
     * @return <tt>'greater than'</tt> predicate
     * @see de.unknownreality.dataframe.filter.ComparePredicate.Operation#GT
     */
    public static FilterPredicate gt(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.GT, value);
    }

    /**
     * Creates a {@link ComparePredicate} using {@link de.unknownreality.dataframe.filter.ComparePredicate.Operation#LT lower than operation}
     *
     * @param name  row column name
     * @param value value for comparison
     * @return <tt>'lower than'</tt> predicate
     * @see de.unknownreality.dataframe.filter.ComparePredicate.Operation#LT
     */
    public static FilterPredicate lt(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.LT, value);

    }

    /**
     * Creates a {@link ComparePredicate} using {@link de.unknownreality.dataframe.filter.ComparePredicate.Operation#GE greater or equal operation}
     *
     * @param name  row column name
     * @param value value for comparison
     * @return <tt>'greater or equal'</tt> predicate
     * @see de.unknownreality.dataframe.filter.ComparePredicate.Operation#GE
     */
    public static FilterPredicate ge(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.GE, value);

    }

    /**
     * Creates a {@link ComparePredicate} using {@link de.unknownreality.dataframe.filter.ComparePredicate.Operation#LE lower or equal operation}
     *
     * @param name  row column name
     * @param value value for comparison
     * @return <tt>'lower or equal'</tt> predicate
     * @see de.unknownreality.dataframe.filter.ComparePredicate.Operation#LE
     */
    public static FilterPredicate le(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.LE, value);
    }

    /**
     * Returns a predicate that checks whether the row column value is contained in an array of comparison values.
     * <p><code>comparison_values.contains(row.getValue(name))</code></p>
     *
     * @param name   row column name
     * @param values values for comparison
     * @return <tt>'in'</tt> predicate.
     */
    public static FilterPredicate in(final String name, final Object[] values) {
        List<String> test = new ArrayList<>();
        Collections.sort(test, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return in(name, Arrays.asList(values));
    }

    /**
     * Returns a predicate that checks whether the row column value is contained in a collection of comparison values.
     * <p><code>comparison_values.contains(row.getValue(name))</code></p>
     *
     * @param name   row column name
     * @param values values for comparison
     * @return <tt>'in'</tt> predicate.
     */
    public static FilterPredicate in(final String name, final Collection<Object> values) {
        return in(name, new HashSet<>(values));
    }

    /**
     * Returns a predicate that checks whether the row column value is contained in a set of comparison values.
     * <p><code>comparison_values.contains(row.getValue(name))</code></p>
     *
     * @param name   row column name
     * @param values values for comparison
     * @return <tt>'in'</tt> predicate.
     */
    public static FilterPredicate in(final String name, final Set<Object> values) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return values.contains(row.get(name));
            }

            @Override
            public String toString() {
                return name + " in " + values.toString();
            }
        };
    }

    /**
     * Returns a predicate that checks whether the row column value is between two comparison values.
     * <p><code>row.get(name) &gt; low AND row.get(name) &lt; high</code></p>
     *
     * @param name row column name
     * @param low  low value
     * @param high high value
     * @return <tt>'between'</tt> predicate.
     */
    public static FilterPredicate btwn(final String name, Object low, Object high) {
        return FilterPredicate.and(FilterPredicate.gt(name, low), FilterPredicate.lt(name, high));
    }

    /**
     * Returns a {@link MatchPredicate} that checks whether the row column value matches a specified {@link Pattern}.
     * <p><code>pattern.matches(row.get(name))</code></p>
     *
     * @param name    row column name
     * @param pattern match pattern
     * @return <tt>'matches'</tt> predicate.
     */
    public static FilterPredicate matches(final String name, Pattern pattern) {
        return new MatchPredicate(name, pattern);
    }

    /**
     * Returns a {@link MatchPredicate} that checks whether the row column value matches a specified pattern string.
     *
     * @param name          row column name
     * @param patternString match pattern string
     * @return <tt>'matches'</tt> predicate.
     * @see #matches(String, Pattern)
     */
    public static FilterPredicate matches(final String name, String patternString) {
        return new MatchPredicate(name, patternString);
    }

}
