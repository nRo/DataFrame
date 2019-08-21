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

package de.unknownreality.dataframe.print;

public class ColumnPrintSettings {
    private Object columnHeader;
    private ValueFormatter valueFormatter;
    private ValueFormatter headerFormatter;
    private Integer width;
    private Integer maxContentWidth;
    private boolean autoWidth;

    public boolean isAutoWidth() {
        return autoWidth;
    }

    public void setAutoWidth(boolean autoWidth) {
        this.autoWidth = autoWidth;
    }

    public ColumnPrintSettings(Object columnHeader){
        this.columnHeader = columnHeader;
    }
    public ColumnPrintSettings(){};

    public Integer getMaxContentWidth() {
        return maxContentWidth;
    }

    public void setMaxContentWidth(Integer maxContentWidth) {
        this.maxContentWidth = maxContentWidth;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Object getColumnName() {
        return columnHeader;
    }

    public void setColumnName(Object columnName) {
        this.columnHeader = columnName;
    }

    public ValueFormatter getValueFormatter() {
        return valueFormatter;
    }

    public void setValueFormatter(ValueFormatter valueFormatter) {
        this.valueFormatter = valueFormatter;
    }

    public Object getColumnHeader() {
        return columnHeader;
    }

    public void setColumnHeader(Object columnHeader) {
        this.columnHeader = columnHeader;
    }

    public ValueFormatter getHeaderFormatter() {
        return headerFormatter;
    }

    public void setHeaderFormatter(ValueFormatter headerFormatter) {
        this.headerFormatter = headerFormatter;
    }
}
