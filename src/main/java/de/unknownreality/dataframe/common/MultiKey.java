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

package de.unknownreality.dataframe.common;

import java.util.Arrays;

/**
 * Created by Alex on 27.05.2016.
 */
public class MultiKey {
    private final Comparable[] values;
    private int hash;

    /**
     * Creates an multi key for an array of {@link Comparable}.
     *
     * @param values key values
     */
    public MultiKey(Comparable[] values) {
        this.values = values;
        updateHash();
    }

    /**
     * Updates the value at a specified index with a new value
     *
     * @param index index of value
     * @param value new value
     */
    public void update(int index, Comparable value) {
        values[index] = value;
        updateHash();
    }


    /**
     * Updates the hash code if this index
     */
    public void updateHash() {
        hash = Arrays.asList(values).hashCode();
    }

    /**
     * Returns true if another object equals this key.
     * The hash is compared in the first step.
     * If both hashes are equal, the values of both keys are compared
     *
     * @param o other object
     * @return <tt>true</tt> if the other object equals this key
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MultiKey)) {
            return false;
        }
        MultiKey other = (MultiKey) o;
        return hashCode() == other.hashCode() && Arrays.equals(values, other.values);
    }

    /**
     * Returns the values in this key
     *
     * @return key values
     */
    public Object[] getValues() {
        return values;
    }

    /**
     * Returns a string representing this key.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    /**
     * Returns the hash code of this key. The hash is based on all key values.
     *
     * @return key hash code
     */
    @Override
    public int hashCode() {
        return hash;
    }
}
