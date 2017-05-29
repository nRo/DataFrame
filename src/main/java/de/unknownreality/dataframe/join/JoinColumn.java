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

package de.unknownreality.dataframe.join;

/**
 * Created by Alex on 12.03.2016.
 */
public class JoinColumn {
    private final String columnA;
    private final String columnB;

    /**
     * Creates a join column for two different column names
     *
     * @param columnA column name in the first data frame
     * @param columnB column name in the second data frame
     */
    public JoinColumn(String columnA, String columnB) {
        this.columnA = columnA;
        this.columnB = columnB;
    }

    /**
     * Creates a join column for the same column name in both data frames
     *
     * @param column column name
     */
    public JoinColumn(String column) {
        this(column, column);
    }

    public String getColumnA() {
        return columnA;
    }

    public String getColumnB() {
        return columnB;
    }
}
