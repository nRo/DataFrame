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


public class IntervalNode<T> {
    private Interval interval;
    private T value;
    private IntervalNode<T> left;
    private IntervalNode<T> right;
    private long subtreeSize;
    private Number max;

    IntervalNode(Interval interval, T value) {
        this.interval = interval;
        this.subtreeSize = 1;
        this.max = interval.high;
        this.value = value;
    }

    public IntervalNode<T> getRight() {
        return right;
    }

    public IntervalNode<T> getLeft() {
        return left;
    }

    public void setRight(IntervalNode<T> right) {
        this.right = right;
    }

    public void setLeft(IntervalNode<T> left) {
        this.left = left;
    }

    public T getValue() {
        return value;
    }

    public Interval getInterval() {
        return interval;
    }

    public Number getMax() {
        return max;
    }

    public long getSubtreeSize() {
        return subtreeSize;
    }

    public void setMax(Number max) {
        this.max = max;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public void setSubtreeSize(long subtreeSize) {
        this.subtreeSize = subtreeSize;
    }

    public int getSubtreeHeight() {
        int l = Integer.MIN_VALUE;
        int r = Integer.MIN_VALUE;
        if (left != null) {
            l = left.getSubtreeHeight();
        }
        if (right != null) {
            r = right.getSubtreeHeight();
        }
        return Math.max(1,Math.max(l,r));
    }
}