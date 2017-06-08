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

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by Alex on 10.03.2016.
 */
public class MultiIterator<T> implements Iterator<T[]>, Iterable<T[]> {

    private final Iterator[] iterators;
    private T[] next;
    private final Class<T> cl;

    /**
     * Creates a multi iterator from an array of iterators
     *
     * @param its array of iterators
     * @param cl  class of entities in iterators
     * @param <T> type of entities in iterators
     * @return multi iterator
     */
    public static <T> MultiIterator<T> create(Iterable<T>[] its, Class<T> cl) {
        return new MultiIterator<>(its, cl);
    }

    @SuppressWarnings("unchecked")
    public static <T> MultiIterator<T> create(Collection<? super Iterable<T>> its, Class<T> cl) {
        Iterable<T>[] itsArray = new Iterable[its.size()];
        its.toArray(itsArray);
        return new MultiIterator<>(itsArray, cl);
    }

    /**
     * Remove is not supported
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported by MultiIterators");
    }


    public MultiIterator(Iterable<T>[] iterables, Class<T> cl) {
        iterators = new Iterator[iterables.length];
        this.cl = cl;
        for (int i = 0; i < iterables.length; i++) {
            iterators[i] = iterables[i].iterator();
        }
        next = getNext();
    }


    /**
     * Returns the next entities array.
     * Each entity iterator provides exactly one entry in this array
     * <tt>null</tt> is returned if at least one iterator has no next element.
     *
     * @return array of entities
     */
    @SuppressWarnings("unchecked")
    private T[] getNext() {
        final T[] next = (T[]) Array.newInstance(cl, iterators.length);
        boolean found = false;
        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                next[i] = (T) iterators[i].next();
                found = true;
            } else {
                next[i] = null;
            }

        }
        if (!found) {
            return null;
        }
        return next;
    }

    /**
     * Returns true if next entities array exists.
     * There is no next entities array if at least one iterator has no next element
     *
     * @return <tt>true</tt> if next entities array exists
     */
    @Override
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next entities array. Each entity iterator provides exactly one entry in this array
     *
     * @return array of entities
     */
    @Override
    public T[] next() {
        T[] rows = next;
        next = getNext();
        return rows;
    }

    /**
     * Returns <tt>self</tt> to be used in foreach loops
     *
     * @return <tt>self</tt>
     */
    @Override
    public Iterator<T[]> iterator() {
        return this;
    }
}
