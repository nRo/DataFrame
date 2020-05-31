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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.row.UpdatableRow;

/**
 * Created by Alex on 12.06.2017.
 */
public class DataRow extends UpdatableRow<String, DataFrameHeader, Object> {
    private DataFrame dataFrame;
    private int size;
    private int rowVersion;

    public DataRow(DataFrame dataFrame, int index) {
        super(dataFrame.getHeader(), index);
        this.rowVersion = dataFrame.getVersion();
        this.size = getHeader().size();
        this.dataFrame = dataFrame;
    }

    public boolean isVersionValid() {
        return rowVersion == dataFrame.getVersion();
    }

    private void checkValidity() {
        if (!isVersionValid()) {
            throw new DataFrameRuntimeException("row is no longer valid, the dataframe changed since the row object was created");
        }
    }

    @Override
    public Object get(int index) {
        checkValidity();
        Object val = dataFrame.getValue(index, getIndex());
        if (val == null) {
            return Values.NA;
        }
        return val;
    }

    public DataFrame getDataFrame() {
        return dataFrame;
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Checks whether a certain value is compatible for a column.
     * Compatible means that the value has a type suited for the respective class
     *
     * @param value      checked value
     * @param headerName header name
     * @return true if value is compatible
     */
    @Override
    public boolean isCompatible(Object value, String headerName) {
        if (value == null || value instanceof Values.NA) {
            return true;
        }
        Class<?> type = getHeader().getValueType(headerName).getType();
        if (Number.class.isAssignableFrom(type)) {
            return Number.class.isAssignableFrom(value.getClass());
        }
        return type.isAssignableFrom(value.getClass());
    }

    /**
     * Checks whether a certain value is compatible for a column.
     * Compatible means that the value has a type suited for the respective class
     *
     * @param value       checked value
     * @param headerIndex header index
     * @return true if value is compatible
     */
    @Override
    public boolean isCompatible(Object value, int headerIndex) {
        Class<?> type = getHeader().getValueType(headerIndex).getType();
        return type.isAssignableFrom(value.getClass());
    }

    @Override
    protected void setValue(int index, Object value) {
        checkValidity();
        dataFrame.setValue(index, getIndex(), value);
    }


    /**
     * Returns the values of a row at a specified index
     *
     * @param i index of data row
     * @return values in data row
     */
    public Object[] getRowValues(int i) {
        checkValidity();
        if (i >= dataFrame.size()) {
            throw new DataFrameRuntimeException("index out of bounds");
        }
        Object[] values = new Comparable[dataFrame.getHeader().size()];
        for (int j = 0; j < dataFrame.getHeader().size(); j++) {
            Object value = dataFrame.getValue(j, i);
            if (Values.NA.isNA(value)) {
                values[j] = Values.NA;
            } else {
                values[j] = value;
            }
        }
        return values;
    }

}
