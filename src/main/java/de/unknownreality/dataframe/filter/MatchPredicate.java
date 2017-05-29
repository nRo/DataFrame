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

import de.unknownreality.dataframe.common.Row;

import java.util.regex.Pattern;

/**
 * Created by Alex on 09.03.2016.
 */
public class MatchPredicate extends FilterPredicate {
    private final Pattern pattern;
    private final String headerName;

    /**
     * Creates a match predicate using a row column name and a pattern string
     *
     * @param headerName    row column name
     * @param patternString pattern string
     */
    public MatchPredicate(String headerName, String patternString) {
        this(headerName, Pattern.compile(patternString));
    }

    /**
     * Creates a match predicate using a row column name and a {@link Pattern}
     *
     * @param headerName row column name
     * @param pattern    input pattern
     */
    public MatchPredicate(String headerName, Pattern pattern) {
        this.headerName = headerName;
        this.pattern = pattern;
    }

    /**
     * Returns <tt>true</tt> if the row column value matches the pattern
     *
     * @param row tested row
     * @return <tt>true</tt> of row column value matches pattern
     */
    @Override
    public boolean valid(Row row) {
        Object v = row.get(headerName);
        return pattern.matcher(v.toString()).matches();
    }

    @Override
    public String toString() {
        return headerName + " =~ /" + pattern.toString() + "/";
    }
}
