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

import de.unknownreality.dataframe.column.BasicColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import de.unknownreality.dataframe.common.row.BasicRow;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.io.ColumnInformation;
import de.unknownreality.dataframe.io.DataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameConverter {
    private static final Logger log = LoggerFactory.getLogger(DataFrameConverter.class);

    public static boolean SAMPLE_ROW_DETECTION = true;
    @SuppressWarnings("unchecked")
    private static Class<? extends Comparable<?>>[] TYPES = new Class[]
            {
                    Boolean.class,
                    Integer.class,
                    Long.class,
                    Double.class
            };
    private static Parser<?>[] TYPE_PARSER = new Parser[]
            {
                    ParserUtil.findParserOrNull(Boolean.class),
                    ParserUtil.findParserOrNull(Integer.class),
                    ParserUtil.findParserOrNull(Long.class),
                    ParserUtil.findParserOrNull(Double.class),

            };

    private DataFrameConverter() {
    }

    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information object.
     * Column information specified by the dataIterator is used.
     * Only rows validated by the filter are appended to the resulting data frame
     *
     * @param <R> row type
     * @param dataIterator    parent data container
     * @param filterPredicate row filter
     * @return created data frame
     */
    public static <R extends Row> DataFrame fromDataIterator(DataIterator<R> dataIterator, FilterPredicate filterPredicate) {
        return fromDataIterator(dataIterator, null, filterPredicate);
    }
    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information object.
     * If no column information is defined, the one specified by the dataIterator is used.
     * Only rows validated by the filter are appended to the resulting data frame
     * @param <R> row type
     * @param dataIterator       parent data container
     * @param columnsInformation column information
     * @param filterPredicate    row filter
     * @return created data frame
     */
    @SuppressWarnings("unchecked")
    public static <R extends Row> DataFrame fromDataIterator(DataIterator<R> dataIterator, List<ColumnInformation> columnsInformation, FilterPredicate filterPredicate) {
        return fromDataIterator(dataIterator,-1,columnsInformation,filterPredicate);
    }
    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information object.
     * If no column information is defined, the one specified by the dataIterator is used.
     * Only rows validated by the filter are appended to the resulting data frame
     * @param <R> row type
     * @param dataIterator       parent data container
     * @param expectedSize       expected size of the resulting dataframe
     * @param columnsInformation column information
     * @param filterPredicate    row filter
     * @return created data frame
     */
    @SuppressWarnings("unchecked")
    public static <R extends Row> DataFrame fromDataIterator(DataIterator<R> dataIterator,int expectedSize, List<ColumnInformation> columnsInformation, FilterPredicate filterPredicate) {

        if (columnsInformation == null) {
            columnsInformation = new ArrayList<>(dataIterator.getColumnsInformation());
        }
        columnsInformation.sort(Comparator.comparingInt(ColumnInformation::getIndex));


        int columnCount = dataIterator.getColumnsInformation().size();
        DataFrame dataFrame = new DefaultDataFrame();
        DataFrameColumn[] columns = new DataFrameColumn[columnCount];
        boolean[] autodetect = new boolean[columns.length];
        boolean[][] types = new boolean[columns.length][TYPES.length];
        boolean hasAutodetect = false;
        for (int i = 0; i < columnCount; i++) {
            ColumnInformation columnInformation = columnsInformation.get(i);
            Class colType = columnInformation.getColumnType();

            DataFrameColumn<?, ?> col;
            try {
                col = (DataFrameColumn<?, ?>) colType.newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
                throw new DataFrameRuntimeException(String.format("error creating instance of column [%s], empty constructor required", colType.getCanonicalName()), e);
            }
            if(expectedSize > BasicColumn.INIT_SIZE){
                col.setCapacity(expectedSize);
            }
            col.setName(columnInformation.getName());
            dataFrame.addColumn(col);
            columns[i] = col;
            autodetect[i] = columnInformation.isAutodetect()
                    && columnInformation.getColumnType().equals(StringColumn.class);
            if (autodetect[i]) {
                hasAutodetect = true;
                for (int j = 0; j < TYPES.length; j++) {
                    types[i][j] = true;
                }
            }
        }
        int r = 0;
        for (R row : dataIterator) {
            Comparable[] rowValues = new Comparable[columnCount];
            for (int i = 0; i < columnCount; i++) {
                ColumnInformation columnInformation = columnsInformation.get(i);
                Comparable val = null;
                if (Values.NA.isNA(row.get(columnInformation.getIndex()))) {
                    rowValues[i] = Values.NA;
                    continue;
                }
                try {
                    val = columns[i].getValueFromRow(row, columnInformation.getIndex());
                } catch (Exception e) {
                    log.warn("error parsing value ({}), NA added", e.getMessage());
                }
                if (val == null || Values.NA.isNA(val) ||
                        val instanceof String && ("".equals(val.toString()) || "null".equals(val.toString()))) {
                    rowValues[i] = Values.NA;
                    continue;
                }
                if (autodetect[i] && (!SAMPLE_ROW_DETECTION || doSample(r))) {
                    for (int j = 0; j < TYPES.length; j++) {
                        types[i][j] = types[i][j]
                                && (TYPE_PARSER[j].parseOrNull(val.toString()) != null);
                    }
                }
                rowValues[i] = val;
            }
            if (hasAutodetect || filterPredicate.valid(new BasicRow(dataFrame.getHeader(),rowValues,dataFrame.size() - 1))) {
                dataFrame.append(rowValues);
            }
            r++;
        }
        if (hasAutodetect) {
            replaceAutodetectColumns(dataFrame, autodetect, types);
            if(filterPredicate != null && filterPredicate != FilterPredicate.EMPTY_FILTER){
                dataFrame.filter(filterPredicate);
            }
        }

        return dataFrame;
    }

    private static boolean doSample(int row){
        if (row < 100) {
            return true;
        }
        if (row < 1000) {
            return row % 10 == 0;
        }
        if (row < 10000) {
            return row % 100 == 0;
        }
        if (row < 100000) {
            return row % 1000 == 0;
        }
        if (row < 1000000) {
            return row % 10000 == 0;
        }
        if (row < 10000000) {
            return row % 100000 == 0;
        }
        if (row < 100000000) {
            return row % 1000000 == 0;
        }
        return row % 10000000 == 0;
    }

    @SuppressWarnings("unchecked")
    private static void replaceAutodetectColumns(DataFrame dataFrame, boolean[] autodetect, boolean[][] types) {
        DataFrameColumn[] newColumns = new DataFrameColumn[autodetect.length];
        List<String> columnNames = new ArrayList<>(dataFrame.getColumnNames());
        for (int i = 0; i < autodetect.length; i++) {
            if (autodetect[i]) {
                Class<? extends Comparable> colType = null;
                for (int j = 0; j < TYPES.length; j++) {
                    if (types[i][j]) {
                        colType = TYPES[j];
                        break;
                    }
                }
                if (colType == null) {
                    continue;
                }
                DataFrameColumn newColumn = ColumnTypeMap.createColumn(colType);
                newColumn.setName(columnNames.get(i));
                newColumn.setCapacity(dataFrame.size());
                newColumns[i] = newColumn;
            }
        }
        int currentRow = 0;
        int currentCol = 0;
        String currentVal = null;
        try {
            for (DataRow row : dataFrame) {
                for (int j = 0; j < autodetect.length; j++) {
                    if (newColumns[j] != null) {
                        currentCol = j;

                        if (row.isNA(j)) {
                            newColumns[j].appendNA();
                            continue;
                        }
                        currentVal = row.getString(j);
                        newColumns[j].append(
                                newColumns[j].getParser().parse(currentVal)
                        );
                    }
                }
                currentRow++;
            }
        } catch (ParseException e) {
            throw new DataFrameRuntimeException(
                    String.format("error parsing value '%s in row %d col %d",
                            currentVal,
                            currentRow,
                            currentCol));
        }
        int i = 0;
        List<DataFrameColumn> columns = new ArrayList<>(dataFrame.getColumns());
        for (DataFrameColumn column : columns) {
            if (newColumns[i] != null) {
                dataFrame.replaceColumn(column, newColumns[i]);
            }
            i++;
        }
    }


    /**
     * Converts a parent data container to a data frame.
     * The required column information is provided by a column information map.
     * Keys in this map are name of the column in the parent data container.
     * Values are the corresponding data frame columns.
     *
     * @param <R> row type
     * @param dataIterator parent data container
     * @return created data frame
     */
    @SuppressWarnings("unchecked")
    public static <R extends Row> DataFrame fromDataIterator(DataIterator<R> dataIterator) {
        return fromDataIterator(dataIterator, FilterPredicate.EMPTY_FILTER);
    }
}
