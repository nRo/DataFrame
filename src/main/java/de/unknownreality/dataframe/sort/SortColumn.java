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

package de.unknownreality.dataframe.sort;

/**
 * Created by Alex on 09.03.2016.
 */
public class SortColumn {
    public enum Direction {
        Ascending,
        Descending
    }

    private final String name;
    private final Direction direction;

    /**
     * Creates a sort column from a column header name and a sort direction
     *
     * @param name      column header name
     * @param direction sort direction
     */
    public SortColumn(String name, Direction direction) {
        this.name = name;
        this.direction = direction;
    }

    /**
     * Creates a sort column from a header name using the default sort direction (ascending)
     *
     * @param name column header name
     */
    public SortColumn(String name) {
        this.name = name;
        this.direction = Direction.Ascending;
    }

    /**
     * Returns the column name
     *
     * @return column name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the sort directions
     *
     * @return sort direction
     */
    public Direction getDirection() {
        return direction;
    }
}
