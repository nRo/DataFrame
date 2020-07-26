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

package de.unknownreality.dataframe.transform;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameException;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.type.DataFrameTypeManager;
import de.unknownreality.dataframe.type.ValueType;

/**
 * Created by Alex on 02.06.2017.
 */
public class StringColumnConverter {
    /**
     * Converts a StringColumn to a other column type by parsing all values
     *
     * @param <V> value type of resulting column
     * @param <C> type of resulting column
     * @param column  original column
     * @param colType target column type
     * @return resulting converted column
     *
     * @throws DataFrameException thrown if conversion not possible
     */
    public static <V, C extends DataFrameColumn<V, C>> C convert(StringColumn column, Class<C> colType) throws DataFrameException {
        if (colType == StringColumn.class) {
            return colType.cast(column.copy());
        }
        C newColumn = colType.cast(DataFrameTypeManager.get().createColumn(colType));
        newColumn.setName(column.getName());
        ValueType<V> valueType = newColumn.getValueType();
        if (valueType == null) {
            throw new DataFrameException(String.format("no parser defined for column type '%s'", colType.getCanonicalName()));
        }
        for (int i = 0; i < column.size(); i++) {
            if (column.isNA(i)) {
                newColumn.appendNA();
                continue;
            }
            V value = newColumn.getValueType().parseOrNull(column.get(i));
            if (value == null) {
                throw new DataFrameException(
                        String.format("error parsing value '%s' -> %s",
                                column.get(i),
                                newColumn.getValueType().getType()));
            } else {
                newColumn.append(value);
            }
        }
        return newColumn;
    }
}
