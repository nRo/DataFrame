/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameConverter {
    private static final Logger log = LoggerFactory.getLogger(DataFrameConverter.class);
    public static final int MAX_PARSER_CACHE_SIZE = 10000;

    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information map.
     * Keys in this map are name of the column in the parent data container.
     * Values are the corresponding data frame columns
     *
     * @param reader parent data container
     * @param columns column information map
     * @return created data frame
     */
    @SuppressWarnings("unchecked")
    public static DataFrame fromDataContainer(DataContainer<?, ?> reader, LinkedHashMap<String, DataFrameColumn> columns) {
        Map<String, Object> parserCache = new HashMap<>();
        int[] colIndices = new int[columns.size()];
        int i = 0;
        for (String h : columns.keySet()) {
            colIndices[i] = reader.getHeader().getIndex(h);
            i++;
        }
        for (Row row : reader) {
            i = 0;
            for (Map.Entry<String, DataFrameColumn> columnEntry : columns.entrySet()) {
                String strVal = row.getString(colIndices[i++]);
                if (strVal == null || "".equals(strVal) || "null".equals(strVal)) {
                    columnEntry.getValue().doAppendNA();
                    continue;
                }
                try {
                    if (Values.NA.isNA(strVal)) {
                        columnEntry.getValue().doAppendNA();
                        continue;
                    }
                    Object o;
                    String object_ident = columnEntry.getValue().getType() + "::" + strVal;
                    if ((o = parserCache.get(object_ident)) == null) {
                        o = columnEntry.getValue().getParser().parse(strVal);
                        if (parserCache.size() > MAX_PARSER_CACHE_SIZE) {
                            parserCache.clear();
                        }
                        parserCache.put(object_ident, o);
                    }
                    if (o == null || !(o instanceof Comparable)) {
                        columnEntry.getValue().doAppendNA();
                        continue;
                    }
                    columnEntry.getValue().append(o);
                } catch (ParseException e) {
                    log.warn("error parsing value, NA added", e);
                    columnEntry.getValue().doAppendNA();
                }
            }
        }
        DataFrame dataFrame = new DataFrame();
        for (DataFrameColumn column : columns.values()) {
            dataFrame.addColumn(column);
        }
        return dataFrame;
    }
}
