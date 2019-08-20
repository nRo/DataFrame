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

import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.common.KeyValueGetter;

import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Alex on 09.03.2016.
 */
public class LikePredicate extends FilterPredicate {
    public enum Type {
        StartsWith,
        EndsWith,
        Contains,
        Equals
    }

    public static final char WILDCARD_CHAR = '_';

    private static final BiFunction<String, String, Boolean> STARTS_WITH_FN = (query, value) -> compareStartsWith(value, query);
    private static final BiFunction<String, String, Boolean> ENDS_WITH_FN = (query, value) -> compareEndsWith(value, query);
    private static final BiFunction<Pattern, String, Boolean> CONTAINS_FN = (pattern, value) -> compareContains(value, pattern);
    private static final BiFunction<String, String, Boolean> EQUALS_FN = (query, value) -> compareEquals(value, query);
    private static final String STARTS_WITH_FORMAT = "%s LIKE '%s%%'";
    private static final String ENDS_WITH_FORMAT = "%s LIKE '%%%s'";
    private static final String CONTAINS_FORMAT = "%s LIKE '%%%s%%'";
    private static final String EQUALS_FORMAT = "%s LIKE '%s'";

    private final String query;
    private final String headerName;
    private Type type;
    private BiFunction<String, String, Boolean> compareFunction;
    private String format;
    private Pattern containsPattern;

    public LikePredicate(String headerName, String query) {
        this.type = findQueryType(query);
        this.query = removeQueryChars(query, type).toLowerCase();
        this.headerName = headerName;
        setup();
    }

    public LikePredicate(String headerName, String query, Type type) {
        this.headerName = headerName;
        this.query = query.toLowerCase();
        this.type = type;
        setup();
    }


    public static Type findQueryType(String query) {
        char c1 = query.charAt(0);
        char c2 = query.charAt(query.length() - 1);
        if (c1 == '%' && c2 == '%') {
            return Type.Contains;
        } else if (c1 == '%') {
            return Type.EndsWith;
        } else if (c2 == '%') {
            return Type.StartsWith;
        }
        return Type.Equals;
    }

    public static String removeQueryChars(String query, Type type) {
        switch (type) {
            case Contains:
                return query.substring(1, query.length() - 1);
            case StartsWith:
                return query.substring(0, query.length() - 1);
            case EndsWith:
                return query.substring(1);
        }
        return query;
    }


    private static boolean compareStartsWith(String value, String query) {
        int ql = query.length();
        for (int i = 0, l = value.length(); i < l; i++) {
            if (i == ql) {
                break;
            }
            char cv = value.charAt(i);
            char cq = query.charAt(i);
            if (cq != '_' && cv != cq) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareEndsWith(String value, String query) {
        int j = query.length() - 1;
        for (int i = value.length() - 1; i >= 0; i--) {
            if (j == -1) {
                break;
            }
            char cv = value.charAt(i);
            char cq = query.charAt(j--);
            if (cq != '_' && cv != cq) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareEquals(String value, String query) {
        int vl = value.length();
        int ql = query.length();
        if (vl != ql) {
            return false;
        }
        for (int i = 0, l = value.length(); i < l; i++) {
            char cv = value.charAt(i);
            char cq = query.charAt(i);
            if (cq != '_' && cv != cq) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareContains(String value, Pattern pattern) {
        return pattern.matcher(value).find();
    }


    private static Pattern createContainsPattern(String query) {
        int l = query.length();
        if (l == 0) {
            return Pattern.compile("");
        }
        StringBuilder sb = new StringBuilder(2 * l);
        for (int i = 0; i < l; i++) {
            char c = query.charAt(i);
            if (
                    c == '[' || c == ']' ||
                            c == '(' || c == ')' ||
                            c == '{' || c == '}' ||
                            c == '.' || c == '*' ||
                            c == '+' || c == '?' ||
                            c == '$' || c == '^' ||
                            c == '|' || c == '#' ||
                            c == '\\'
            ) {
                sb.append("\\");
                sb.append(c);
            } else if (c == WILDCARD_CHAR) {
                sb.append(".");
            } else {
                sb.append(c);
            }

        }
        return Pattern.compile(sb.toString());
    }

    private void setup() {
        switch (this.type) {
            case Contains:
                this.containsPattern = createContainsPattern(query);
                this.compareFunction = (q, v) -> compareContains(v, containsPattern);
                this.format = CONTAINS_FORMAT;
                break;
            case EndsWith:
                this.compareFunction = ENDS_WITH_FN;
                this.format = ENDS_WITH_FORMAT;
                break;
            case StartsWith:
                this.compareFunction = STARTS_WITH_FN;
                this.format = STARTS_WITH_FORMAT;
                break;
            default:
                this.compareFunction = EQUALS_FN;
                this.format = EQUALS_FORMAT;
        }
    }

    @Override
    public boolean valid(KeyValueGetter<String, ?> kv) {
        Object v = kv.get(headerName);
        if (Values.NA.isNA(v)) {
            return false;
        }
        String value = v.toString();

        value = value.toLowerCase();

        return compareFunction.apply(query, value);
    }

    @Override
    public String toString() {
        return String.format(format, headerName, query);
    }


}
