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
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Alex on 08.06.2016.
 */
public class DataFrameLoader {


    private DataFrameLoader(){}
    /**
     * Loads a data frame from a file.
     * The matching data frame meta file must be present.
     * Only rows validated by the filter are appended to the resulting data frame
     * <code>file+'.dfm'</code>
     *
     * @param file            data frame file
     * @param filterPredicate row filter
     * @return loaded data frame
     * @throws DataFrameException thrown if the data frame can not be loaded
     */
    public static DataFrame load(File file, FilterPredicate filterPredicate) throws DataFrameException {
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
        return load(dataFile, metaFile, filterPredicate);
    }

    /**
     * Loads a data frame from a file.
     * The matching data frame meta file must be present.
     * <code>file+'.dfm'</code>
     *
     * @param file data frame file
     * @return loaded data frame
     * @throws DataFrameException thrown if the data frame can not be loaded
     */
    public static DataFrame load(File file) throws DataFrameException {
        return load(file, FilterPredicate.EMPTY_FILTER);
    }

    /**
     * Loads a data frame from a file and the corresponding meta file.
     * Only rows validated by the filter are appended to the resulting data frame
     *
     * @param file            data frame file
     * @param metaFile        data frame meta file
     * @param filterPredicate row filter
     * @return loaded data frame
     * @throws DataFrameException thrown if the data frame can not be loaded
     */

    public static DataFrame load(File file, File metaFile, FilterPredicate filterPredicate) throws DataFrameException {
        if (!file.exists()) {
            throw new DataFrameException(String.format("file not found %s", file.getAbsolutePath()));
        }
        if (!metaFile.exists()) {
            throw new DataFrameException(String.format("meta file not found %s", metaFile.getAbsolutePath()));
        }

        DataFrameMeta dataFrameMeta;
        dataFrameMeta = DataFrameMetaReader.read(metaFile);

        ReaderBuilder readerBuilder;
        try {
            readerBuilder = dataFrameMeta.getReaderBuilderClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameException("error creating readerBuilder instance", e);
        }
        try {
            readerBuilder.loadAttributes(dataFrameMeta.getAttributes());
        } catch (Exception e) {
            throw new DataFrameException("error loading readerBuilder attributes", e);
        }
        Map<String, DataFrameColumn> columns = createColumns(dataFrameMeta);
        DataContainer fileContainer = readerBuilder.fromFile(file);
        Map<String, DataFrameColumn> convertedColumns = convertColumns(columns, fileContainer);
        return DataFrameConverter.fromDataContainer(fileContainer, convertedColumns, filterPredicate);
    }

    /**
     * Loads a data frame from a file and the corresponding meta file.
     *
     * @param file     data frame file
     * @param metaFile data frame meta file
     * @return loaded data frame
     * @throws DataFrameException thrown if the data frame can not be loaded
     */
    public static DataFrame load(File file, File metaFile) throws DataFrameException {
        return load(file, metaFile, FilterPredicate.EMPTY_FILTER);
    }

    /**
     * Loads a data frame from a resource and the corresponding meta resource.
     * Only rows validated by the filter are appended to the resulting data frame
     *
     * @param path            path to data frame resource
     * @param metaPath        path to  meta file resoure
     * @param classLoader     class loader for the resource
     * @param filterPredicate row filter
     * @return loaded data frame
     * @throws DataFrameException thrown if the data frame can not be loaded
     */

    public static DataFrame loadResource(String path, String metaPath, ClassLoader classLoader, FilterPredicate filterPredicate) throws DataFrameException {

        DataFrameMeta dataFrameMeta;
        dataFrameMeta = DataFrameMetaReader.read(classLoader.getResourceAsStream(metaPath));

        ReaderBuilder readerBuilder;
        try {
            readerBuilder = dataFrameMeta.getReaderBuilderClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameException("error creating readerBuilder instance", e);
        }
        try {
            readerBuilder.loadAttributes(dataFrameMeta.getAttributes());
        } catch (Exception e) {
            throw new DataFrameException("error loading readerBuilder attributes", e);
        }
        Map<String, DataFrameColumn> columns = createColumns(dataFrameMeta);
        DataContainer fileContainer = readerBuilder.fromResource(path, classLoader);
        Map<String, DataFrameColumn> convertedColumns = convertColumns(columns, fileContainer);
        return DataFrameConverter.fromDataContainer(fileContainer, convertedColumns, filterPredicate);
    }

    /**
     * Loads a data frame from a resource and the corresponding meta resource.
     *
     * @param path        path to data frame resource
     * @param metaPath    path to  meta file resoure
     * @param classLoader class loader for the resource
     * @return loaded data frame
     * @throws DataFrameException thrown if the data frame can not be loaded
     */
    public static DataFrame loadResource(String path, String metaPath, ClassLoader classLoader) throws DataFrameException {
        return loadResource(path, metaPath, classLoader, FilterPredicate.EMPTY_FILTER);
    }

    private static Map<String, DataFrameColumn> convertColumns(Map<String, DataFrameColumn> columns, DataContainer fileContainer) throws DataFrameException {
        int i = 0;

        //Fix for empty data frames
        if(fileContainer.getHeader().size() == 0){
            return columns;
        }
        LinkedHashMap<String, DataFrameColumn> convertedColumns = new LinkedHashMap<>();
        for (Map.Entry<String, DataFrameColumn> entry : columns.entrySet()) {
            if (i == fileContainer.getHeader().size()) {
                throw new DataFrameException("columns count not matching meta file");
            }
            convertedColumns.put(fileContainer.getHeader().get(i).toString(), entry.getValue());
            i++;
        }
        return convertedColumns;
    }

    /**
     * Creates the columns from a data frame meta information
     *
     * @param dataFrameMeta meta information
     * @return data frame columns
     * @throws DataFrameException thrown if the columns can not be created
     */
    public static Map<String, DataFrameColumn> createColumns(DataFrameMeta dataFrameMeta) throws DataFrameException {
        LinkedHashMap<String, DataFrameColumn> columns = new LinkedHashMap<>();
        for (Map.Entry<String, Class<? extends DataFrameColumn>> entry : dataFrameMeta.getColumns().entrySet()) {
            String name = entry.getKey();
            Class<? extends DataFrameColumn> columnType = entry.getValue();
            DataFrameColumn column;
            try {
                column = columnType.newInstance();
            } catch (Exception e) {
                throw new DataFrameException("error creating column instance", e);
            }
            column.setName(name);
            columns.put(name, column);
        }
        return columns;
    }


}
