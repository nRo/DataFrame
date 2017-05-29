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

package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataFrame;

/**
 * Created by Alex on 10.03.2016.
 */
public class DataGroup extends DataFrame {
    private GroupHeader groupHeader;
    private GroupValues groupValues;

    /**
     * Creates a data grouping using group columns and the respective values
     *
     * @param columns group columns
     * @param values  group column values
     */
    public DataGroup(String[] columns, Comparable[] values) {
        if (columns.length != values.length) {
            throw new IllegalArgumentException("column and values must have same length");
        }
        groupHeader = new GroupHeader(columns);
        Comparable[] groupValueArray = new Comparable[values.length];
        System.arraycopy(values, 0, groupValueArray, 0, values.length);
        this.groupValues = new GroupValues(groupValueArray, groupHeader);
    }

    /**
     * Returns the {@link GroupHeader}
     *
     * @return group header
     */
    public GroupHeader getGroupHeader() {
        return groupHeader;
    }

    /**
     * Returns the {@link GroupValues}
     *
     * @return group values
     */
    public GroupValues getGroupValues() {
        return groupValues;
    }

    /**
     * Returns the group description.
     * The description is based on the group columns and their respective value.
     * <p><code>groupColumnA=groupValueA, groupColumnB=groupValueB, ...</code></p>
     *
     * @return group description
     */
    public String getGroupDescription() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String h : groupHeader) {
            sb.append(h).append("=").append(groupValues.get(h));
            if (i++ < groupHeader.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }


}



