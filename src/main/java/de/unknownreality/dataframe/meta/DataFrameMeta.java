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

package de.unknownreality.dataframe.meta;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.io.ColumnInformation;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.ReadFormat;
import de.unknownreality.dataframe.io.ReaderBuilder;

import java.util.*;

/**
 * Created by Alex on 07.06.2016.
 */
public class DataFrameMeta {
    public static final String META_FILE_EXTENSION = "dfm";

    private Class<? extends ReadFormat> readFormatClass;
    private Map<String, String> attributes = new HashMap<>();
    private Map<String, Class<? extends DataFrameColumn>> columns = new LinkedHashMap<>();

    /**
     * Creates data frame meta information
     *
     * @param dataFrame          source data frame
     * @param readFormatClass class of used read format
     * @param dataWriterBuilder  data writer used
     * @return data frame meta information
     */
    public static DataFrameMeta create(DataFrame dataFrame, Class<? extends ReadFormat> readFormatClass, DataWriter dataWriterBuilder) {
        return create(dataFrame, readFormatClass, dataWriterBuilder.getSettings());
    }

    /**
     * Creates data frame meta information
     *
     * @param dataFrame          source data frame
     * @param readFormatClass class of used read format
     * @param writerAttributes   attributes of the used data writer
     * @return data frame meta information
     */
    public static DataFrameMeta create(DataFrame dataFrame, Class<? extends ReadFormat> readFormatClass, Map<String, String> writerAttributes) {
        DataFrameMeta dataFrameMetaFile = new DataFrameMeta();
        dataFrameMetaFile.readFormatClass = readFormatClass;
        dataFrameMetaFile.attributes = writerAttributes;
        for (String header : dataFrame.getHeader()) {
            dataFrameMetaFile.columns.put(header, dataFrame.getHeader().getColumnType(header));
        }
        return dataFrameMetaFile;
    }

    /**
     * Returns the {@link ReaderBuilder} class that can be used to read the data frame
     *
     * @return reader builder class
     */
    public Class<? extends ReadFormat> getReadFormatClass() {
        return readFormatClass;
    }

    /**
     * Returns a map of the columns in the data frame.
     * The keys are the names of the columns. Values are the types of the columns
     *
     * @return column name/type map
     */
    public Map<String, Class<? extends DataFrameColumn>> getColumns() {
        return columns;
    }

    /**
     * Returns the attributes from the data writer used to write the data frame
     *
     * @return attribute map for reader builder
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    public List<ColumnInformation> getColumnInformation(){
        List<ColumnInformation> columnInformations = new ArrayList<>();
        int i = 0;
        for(Map.Entry<String, Class<? extends DataFrameColumn>> e : columns.entrySet()){
            ColumnInformation information = new ColumnInformation(i++, e.getKey());
            information.setColumnType(e.getValue());
            columnInformations.add(information);
        }
        return columnInformations;
    }

    public DataFrameMeta() {

    }

    public DataFrameMeta(Map<String, Class<? extends DataFrameColumn>> columns,
                         Class<? extends ReadFormat> readFormatClass, Map<String, String> attributes) {
        this.columns = columns;
        this.readFormatClass = readFormatClass;
        this.attributes = attributes;
    }
}
