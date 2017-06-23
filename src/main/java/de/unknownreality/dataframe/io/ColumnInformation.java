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

package de.unknownreality.dataframe.io;

import de.unknownreality.dataframe.ColumnTypeMap;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.column.StringColumn;

/**
 * Created by Alex on 17.06.2017.
 */
public class ColumnInformation {
    private boolean autodetect;
    private String name;
    private int index;
    private Class<? extends DataFrameColumn> columnType;


    public ColumnInformation(int index, String name, Class<? extends Comparable> type){
        this.index = index;
        this.name = name;
        this.columnType = ColumnTypeMap.get(type);
        if (columnType == null) {
            throw new DataFrameRuntimeException(String.format("no column type found for value type '%s'", type));
        }
        this.autodetect = false;
    }

    public ColumnInformation(int index, String name){
        this.index = index;
        this.name = name;
        this.autodetect = false;
    }

    public void setColumnType(Class<? extends DataFrameColumn> columnType) {
        this.columnType = columnType;
    }

    public ColumnInformation(int index, String name, boolean autodetect){
        this.index = index;
        this.name = name;
        this.columnType = StringColumn.class;
        this.autodetect = autodetect;
    }
    public boolean isAutodetect() {
        return autodetect;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public Class<? extends DataFrameColumn> getColumnType() {
        return columnType;
    }
}
