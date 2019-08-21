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

package de.unknownreality.dataframe.index.interval;

import static de.unknownreality.dataframe.common.NumberUtil.*;

public class Interval implements Comparable<Interval> {
    public final Number low;
    public final Number high;

    public Interval(Number left, Number right) {
        this.low  = left;
        this.high = right;
    }

    public Number getLow() {
        return low;
    }

    public Number getHigh() {
        return high;
    }

    public boolean contains(Number value) {
        return ge(value,low) && le(value,high);
    }

    public boolean intersects(Interval interval){
        return le(low, interval.high) && ge(high, interval.low);
    }

    public int compareTo(Interval interval) {
        int c = compare(low, interval.getLow());
        if(c == 0){
            return compare(high, interval.getHigh());
        }
        return c;
    }


    public String toString() {
        return "[" + low + ", " + high + "]";
    }
}