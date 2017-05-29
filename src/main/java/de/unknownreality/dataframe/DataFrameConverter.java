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

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.filter.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameConverter {
    private static final Logger log = LoggerFactory.getLogger(DataFrameConverter.class);

    private DataFrameConverter(){}
    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information map.
     * Keys in this map are name of the column in the parent data container.
     * Values are the corresponding data frame columns.
     * Only rows validated by the filter are appended to the resulting data frame
     *
     * @param reader          parent data container
     * @param columns         column information map
     * @param filterPredicate row filter
     * @return created data frame
     */
    @SuppressWarnings("unchecked")
    public static DataFrame fromDataContainer(DataContainer<?, ?> reader, Map<String, DataFrameColumn> columns, FilterPredicate filterPredicate) {
        if(reader.getHeader().size() == 0){
            DataFrame dataFrame = new DataFrame();
            for(DataFrameColumn column : columns.values()){
                dataFrame.addColumn(column);
            }
            return dataFrame;
        }
        int[] colIndices = new int[columns.size()];
        int i = 0;
        for (String h : columns.keySet()) {
            colIndices[i] = reader.getHeader().getIndex(h);
            i++;
        }
        DataFrame dataFrame = new DataFrame();
        for (DataFrameColumn column : columns.values()) {
            dataFrame.addColumn(column);
        }
        for (Row row : reader) {
            i = -1;
            Comparable[] rowValues = new Comparable[columns.size()];
            for (Map.Entry<String, DataFrameColumn> columnEntry : columns.entrySet()) {
                i++;
                Comparable val = null;
                try {
                    val = columnEntry.getValue().getValueFromRow(row, colIndices[i]);
                }
                catch (Exception e){
                    log.warn("error parsing value ({}), NA added",e.getMessage());
                }
                if(val == null || Values.NA.isNA(val) ||
                        val instanceof String && ("".equals(val.toString()) || "null".equals(val.toString()))){
                    rowValues[i] = Values.NA;
                    continue;
                }
                rowValues[i] = val;
            }
            DataRow dataRow = new DataRow(dataFrame.getHeader(),rowValues,dataFrame.size() - 1);
            if(filterPredicate.valid(dataRow)){
                dataFrame.append(dataRow);
            }
        }

        return dataFrame;
    }

    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information map.
     * Keys in this map are name of the column in the parent data container.
     * Values are the corresponding data frame columns.
     *
     * @param reader          parent data container
     * @param columns         column information map
     * @return created data frame
     */
    @SuppressWarnings("unchecked")
    public static DataFrame fromDataContainer(DataContainer<?, ?> reader, Map<String, DataFrameColumn> columns) {
        return fromDataContainer(reader,columns,FilterPredicate.EMPTY_FILTER);
    }
}
