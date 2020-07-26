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

package de.unknownreality.dataframe.column;


import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.MapFunction;
import de.unknownreality.dataframe.Values;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class BasicColumn<T, C extends BasicColumn<T, C>> extends DataFrameColumn<T, C> {
    public static final double GROW_FACTOR = 1.6d;
    public static final int INIT_SIZE = 128;

    private int size = 0;

    protected T[] values;

    @SuppressWarnings("unchecked")
    public BasicColumn(String name, Class<T> cl) {
        this.size = 0;
        setName(name);
        values = (T[]) Array.newInstance(cl, INIT_SIZE);
    }

    public BasicColumn(Class<T> cl) {
        this(null, cl);
    }

    public BasicColumn(String name, T[] values, int size) {
        this.values = values;
        setName(name);
        this.size = size;
    }

    public BasicColumn(String name, T[] values) {
        this(name, values, values.length);
    }

    @Override
    public C setCapacity(int capacity) {
        if (capacity < size) {
            throw new DataFrameRuntimeException("capacity can not be lower than current size");
        }
        values = Arrays.copyOf(values, capacity);
        return getThis();
    }

    @Override
    protected void doSort(Comparator<T> comparator) {
        Arrays.sort(values, 0, size(), comparator);
    }


    @Override
    protected void doSort() {
        Arrays.sort(values, 0, size(), getValueType().getComparator());

    }


    @Override
    protected void doSet(int index, T value) {
        if (value == null || value == Values.NA) {
            doSetNA(index);
            return;
        }
        setValue(index, value);
    }

    protected void setValue(int index, T value) {
        values[index] = value;
    }


    @Override
    protected void doMap(MapFunction<T> mapFunction) {
        for (int i = 0; i < size(); i++) {
            if (isNA(i)) {
                continue;
            }
            values[i] = mapFunction.map(values[i]);
        }
    }


    @Override
    protected void doReverse() {
        for (int i = 0; i < size() / 2; i++) {
            T temp = values[i];
            values[i] = values[size() - i - 1];
            values[size() - i - 1] = temp;
        }
    }


    @Override
    public T get(int index) {
        return values[index];
    }

    @Override
    public boolean isValueValid(Object value) {
        return Values.NA.isNA(value) ||
                getValueType().getType().isAssignableFrom(value.getClass());
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(T o) {
        return Arrays.asList(values).contains(o);
    }

    /**
     * Returns an iterator over the values in this column.
     * {@link Iterator#remove()} is not supported
     *
     * @return iterator over column values
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int index = 0;

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported by this iterator");
            }

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public T next() {
                if (index >= values.length) {
                    throw new NoSuchElementException(String.format("element not found: index out of bounds %s >= %s]", index, values.length));
                }
                return values[index++];
            }
        };
    }

    /**
     * Returns a set containing all values in this column
     *
     * @return set of values in this column
     */
    public Set<T> uniq() {
        Set<T> u = new HashSet<>(Arrays.asList(values));
        u.remove(null);
        return u;
    }


    @Override
    public T[] toArray() {
        return Arrays.copyOf(values, size());
    }


    @SuppressWarnings("unchecked")
    @Override
    public T[] toArray(T[] a) {
        if (a.length < size())
            return (T[]) Arrays.copyOf(values, size(), a.getClass());
        System.arraycopy(values, 0, a, 0, size());
        if (a.length > size())
            a[size()] = null;
        return a;
    }


    @Override
    protected boolean doAppend(T t) {

        if (size >= values.length - 1) {
            values = Arrays.copyOf(values, (int) ((double) values.length * GROW_FACTOR));
        }
        values[size++] = t;
        return true;
    }

    @Override
    protected boolean doAppendNA() {
        return doAppend(null);
    }

    @Override
    public boolean isNA(int index) {
        return values.length <= index || values[index] == null;
    }


    @Override
    protected void doSetNA(int index) {
        values[index] = null;
    }


    @Override
    public boolean containsAll(Collection<?> c) {
        return new HashSet<>(Arrays.asList(values)).containsAll(c);
    }


    @Override
    protected boolean doAppendAll(Collection<? extends T> c) {
        for (T o : c) {
            doAppend(o);
        }
        return true;
    }

    @Override
    public List<T> toList() {
        return new ArrayList<>(Arrays.asList(Arrays.copyOf(values, size)));
    }

    @Override
    public List<T> asList() {
        return Collections.unmodifiableList(
                new BasicValueList<>(values, size)
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() {
        values = (T[]) Array.newInstance(getValueType().getType(), INIT_SIZE);
        size = 0;
    }

    static class BasicValueList<E> extends AbstractList<E>
            implements RandomAccess, java.io.Serializable {
        private static final long serialVersionUID = -2764017481108945198L;
        private final E[] a;
        private final int size;

        BasicValueList(E[] array, int size) {
            a = Objects.requireNonNull(array);
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Object[] toArray() {
            return Arrays.copyOf(a, size, Object[].class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] a) {
            int size = size();
            if (a.length < size)
                return Arrays.copyOf(this.a, size,
                        (Class<? extends T[]>) a.getClass());
            System.arraycopy(this.a, 0, a, 0, size);
            if (a.length > size)
                a[size] = null;
            return a;
        }

        @Override
        public E get(int index) {
            return a[index];
        }

        @Override
        public int indexOf(Object o) {
            E[] a = this.a;
            if (o == null) {
                for (int i = 0; i < size; i++)
                    if (a[i] == null)
                        return i;
            } else {
                for (int i = 0; i < size; i++)
                    if (o.equals(a[i]))
                        return i;
            }
            return -1;
        }
    }
}
