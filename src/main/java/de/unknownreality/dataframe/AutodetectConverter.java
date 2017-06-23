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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;

import java.text.ParseException;

/**
 * Created by Alex on 21.06.2017.
 */
public class AutodetectConverter {
    private static Class<?>[] CLASSES = new Class[]
            {
                    Boolean.class,
                    Integer.class,
                    Long.class,
                    Double.class
            };
    private static Parser<?>[] PARSER = new Parser[]
            {
                    ParserUtil.findParserOrNull(Boolean.class),
                    ParserUtil.findParserOrNull(Integer.class),
                    ParserUtil.findParserOrNull(Long.class),
                    ParserUtil.findParserOrNull(Double.class),

            };

    public DataFrameColumn<?, ?> convert(StringColumn column) {
        return convert(column, column.size());
    }

    public DataFrameColumn<?, ?> convert(StringColumn column, int testLength) {
        Class<? extends Comparable>[] remainingTypes = new Class[CLASSES.length];
        System.arraycopy(CLASSES, 0, remainingTypes, 0, CLASSES.length);
        int remaining = CLASSES.length;
        int setLength = Math.min(column.size(), testLength);
        for (int i = 0; i < setLength; i++) {
            if (column.isNA(i)) {
                continue;
            }
            String s = column.get(i);
            for(int j = 0; j < remainingTypes.length; j++){
                if (remainingTypes[j] == null) {
                    continue;
                }
                if (PARSER[j].parseOrNull(s) == null) {
                    remainingTypes[j] = null;
                    remaining--;
                    if (remaining == 0) {
                        i = setLength;
                        break;
                    }
                }
            }

        }
        if (remaining == 0) {
            return column;
        }
        Class<? extends Comparable> type = null;
        Parser<?> parser = null;
        for (int i = 0; i < remainingTypes.length; i++) {
            if (remainingTypes[i] != null) {
                type = remainingTypes[i];
                parser = PARSER[i];
                break;
            }
        }
        Class<? extends DataFrameColumn> colType = ColumnTypeMap.get(type);
        DataFrameColumn newColumn = null;
        try {
            newColumn = colType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DataFrameRuntimeException(String.format("error creating column instance (%s)", colType.getCanonicalName()));
        }
        newColumn.setName(column.getName());
        try {
            for (int i = 0; i < column.size(); i++) {
                if (column.isNA(i)) {
                    newColumn.appendNA();
                } else {

                    newColumn.append(parser.parse(column.get(i)));

                }
            }
        } catch (ParseException e) {
            throw new DataFrameRuntimeException(String.format("error parsing value", e));
        }
        return newColumn;
    }
}
