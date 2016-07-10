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
import de.unknownreality.dataframe.common.ReaderBuilder;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Alex on 08.06.2016.
 */
public class DataFrameLoader {
    private static Logger logger = LoggerFactory.getLogger(DataFrameLoader.class);

    /**
     * Loads a data frame from a file.
     * The matching data frame meta file must be present.
     * <code>file+'.dfm'</code>
     * @param file data frame file
     * @return loaded data frame
     * @throws DataFrameLoaderException thrown if the data frame can not be loaded
     */
    public static DataFrame load(File file) throws DataFrameLoaderException {
        File dataFile;
        File metaFile;
        String ext = "." + DataFrameMeta.META_FILE_EXTENSION;
        String filePath = file.getAbsolutePath();
        if (file.getName().endsWith(ext)) {
            metaFile = file;
            dataFile = new File(filePath
                    .substring(0, filePath.length() - ext.length()));
        } else {
            dataFile = file;
            metaFile = new File(filePath + ext);
        }
        return load(dataFile, metaFile);
    }

    /**
     * Loads a data frame from a file and the corresponding meta file.

     * @param file data frame file
     * @param metaFile data frame meta file
     * @return loaded data frame
     * @throws DataFrameLoaderException thrown if the data frame can not be loaded
     */
    public static DataFrame load(File file, File metaFile) throws DataFrameLoaderException {
        if (!file.exists()) {
            throw new DataFrameLoaderException(String.format("file not found %s", file.getAbsolutePath()));
        }
        if (!metaFile.exists()) {
            throw new DataFrameLoaderException(String.format("meta file not found %s", metaFile.getAbsolutePath()));

        }
        DataFrameMeta dataFrameMeta;
        try {
            dataFrameMeta = DataFrameMetaReader.read(metaFile);
        } catch (DataFrameMetaReader.DataFrameMetaReaderException ex) {
            throw new DataFrameLoaderException("error reading meta file", ex);
        }

        ReaderBuilder readerBuilder;
        try {
            readerBuilder = dataFrameMeta.getReaderBuilderClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameLoaderException("error creating readerBuilder instance", e);
        }
        try {
            readerBuilder.loadAttributes(dataFrameMeta.getAttributes());
        } catch (Exception e) {
            throw new DataFrameLoaderException("error loading readerBuilder attributes", e);
        }
        LinkedHashMap<String, DataFrameColumn> columns = createColumns(dataFrameMeta);
        DataContainer fileContainer = readerBuilder.fromFile(file);
        int i = 0;
        LinkedHashMap<String, DataFrameColumn> convertedColumns = new LinkedHashMap<>();
        for (Map.Entry<String, DataFrameColumn> entry : columns.entrySet()) {
            if (i == fileContainer.getHeader().size()) {
                throw new DataFrameLoaderException("columns count not matching meta file");
            }
            convertedColumns.put(fileContainer.getHeader().get(i).toString(), entry.getValue());
            i++;
        }
        return DataFrameConverter.fromDataContainer(fileContainer, convertedColumns);
    }

    /**
     * Creates the columns from a data frame meta information
     * @param dataFrameMeta meta information
     * @return data frame columns
     * @throws DataFrameLoaderException thrown if the columns can not be created
     */
    public static LinkedHashMap<String, DataFrameColumn> createColumns(DataFrameMeta dataFrameMeta) throws DataFrameLoaderException {
        LinkedHashMap<String, DataFrameColumn> columns = new LinkedHashMap<>();
        for (Map.Entry<String, Class<? extends DataFrameColumn>> entry : dataFrameMeta.getColumns().entrySet()) {
            String name = entry.getKey();
            Class<? extends DataFrameColumn> columnType = entry.getValue();
            DataFrameColumn column;
            try {
                column = columnType.newInstance();
            } catch (Exception e) {
                throw new DataFrameLoaderException("error creating column instance", e);
            }
            column.setName(name);
            columns.put(name, column);
        }
        return columns;
    }


    public static class DataFrameLoaderException extends DataFrameException {

        public DataFrameLoaderException(String message) {
            super(message);
        }

        public DataFrameLoaderException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }
}
