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

package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.NumberUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by Alex on 11.03.2016.
 */
public abstract class NumberColumn<T extends Number & Comparable<T>, C extends NumberColumn<T, C>> extends BasicColumn<T, C> {
    private static final Logger log = LoggerFactory.getLogger(NumberColumn.class);

    public NumberColumn(String name) {
        super(name);
    }

    public NumberColumn() {
        super(null);
    }

    public NumberColumn(String name, T[] values) {
        super(name, values);
    }

    @Override
    public T get(int index) {
        return super.values[index];
    }


    /**
     * Returns the median of all values in this column
     *
     * @return median of all values
     */
    public T median() {
        Comparable[] array = toArray();
        Arrays.sort(array);
        return NumberUtil.convert((Number) array[size() / 2], getType());
    }


    /**
     * Returns the mean of all values in this column
     *
     * @return mean of all values
     */
    public Double mean() {
        int naCount = 0;
        Double sum = 0d;
        int count = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (isNA(i)) {
                naCount++;
                continue;
            }
            count++;
            sum += get(i).doubleValue();
        }
        if (naCount > 0) {
            log.warn("mean() ignored {} NA", naCount);
        }
        return sum / count;
    }

    /**
     * Returns the minimum of all values in this column
     *
     * @return minimum of all values
     */
    public T min() {
        Double min = Double.MAX_VALUE;
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (isNA(i)) {
                naCount++;
                continue;
            }
            min = Math.min(min, get(i).doubleValue());
        }
        if (naCount > 0) {
            log.warn("min() ignored {} NA", naCount);
        }
        return NumberUtil.convert(min, getType());
    }

    /**
     * Returns the maximum of all values in this column
     *
     * @return maximum of all values
     */
    public T max() {
        double max = Double.NEGATIVE_INFINITY;
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (isNA(i)) {
                naCount++;
                continue;
            }
            max = Math.max(max, get(i).doubleValue());
        }
        if (naCount > 0) {
            log.warn("max() ignored {} NA", naCount);
        }
        return NumberUtil.convert(max, getType());
    }

    /**
     * Returns the sum of all values in this column
     *
     * @return sum of all values
     */
    public T sum() {
        int naCount = 0;
        double sum = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (isNA(i)) {
                naCount++;
                continue;
            }
            sum += get(i).doubleValue();
        }
        if (naCount > 0) {
            log.warn("sum() ignored {} NA", naCount);
        }
        return NumberUtil.convert(sum, getType());
    }


    /**
     * Adds the values of another {@link NumberColumn} to the values in this column.
     * {@code column[index] += otherColumn[index]}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param column column containing the values that are added
     * @return <tt>self</tt> for method chaining
     */
    public C add(NumberColumn column) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && !column.isNA(i)) {
                doSet(i, NumberUtil.add(get(i), column.get(i), getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("add() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }


    /**
     * Subtracts the values of another {@link NumberColumn} from the values in this column.
     * {@code column[index] -= otherColumn[index]}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param column column containing the values that are subtracted
     * @return <tt>self</tt> for method chaining
     */
    public C subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' requires column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && !column.isNA(i)) {
                doSet(i, NumberUtil.subtract(get(i), column.get(i), getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("subtract() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Multiplies the values of another {@link NumberColumn} to the values in this column.
     * {@code column[index] *= otherColumn[index]}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param column column containing the values that are multiplied
     * @return <tt>self</tt> for method chaining
     */
    public C multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' requires column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && !column.isNA(i)) {
                doSet(i, NumberUtil.multiply(get(i), column.get(i), getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("multiply() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Divides the values of this column by the values of another {@link NumberColumn}.
     * {@code column[index] /= otherColumn[index]}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param column column containing the values that are divided
     * @return <tt>self</tt> for method chaining
     */
    public C divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' requires column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && !column.isNA(i)) {
                doSet(i, NumberUtil.divide(get(i), column.get(i), getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("divide() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }


    /**
     * Adds a {@link Number} to the values in this column.
     * {@code column[index] += number}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param value value added to all values in this column
     * @return <tt>self</tt> for method chaining
     */
    public C add(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && value != null) {
                doSet(i, NumberUtil.add(get(i), value, getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("add() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Subtracts a {@link Number} to the values in this column.
     * {@code column[index] -= number}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param value value subtracted from all values in this column
     * @return <tt>self</tt> for method chaining
     */
    public C subtract(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && value != null) {
                doSet(i, NumberUtil.subtract(get(i), value, getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("subtract() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }


    /**
     * Multiplies a {@link Number} to the values in this column.
     * {@code column[index] *= number}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param value value multiplied to all values in this column
     * @return <tt>self</tt> for method chaining
     */
    public C multiply(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && value != null) {
                doSet(i, NumberUtil.multiply(get(i), value, getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("multiply() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }

    /**
     * Divides all values in this column by a {@link Number}.
     * {@code column[index] /= number}
     * <p>Calls {@link #notifyDataFrameValueChanged(int)} to ensure data frame index consistency</p>
     *
     * @param value the value all values in this column are divided by
     * @return <tt>self</tt> for method chaining
     */
    public C divide(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if (!isNA(i) && value != null) {
                doSet(i, NumberUtil.divide(get(i), value, getType()));
            } else {
                naCount++;
            }
        }
        if (naCount > 0) {
            log.warn("divide() ignored {} NA", naCount);
        }
        notifyDataFrameColumnChanged();
        return getThis();
    }
}
