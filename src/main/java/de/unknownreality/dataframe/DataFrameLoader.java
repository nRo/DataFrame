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

import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.io.DataIterator;
import de.unknownreality.dataframe.io.DataReader;
import de.unknownreality.dataframe.io.ReadFormat;
import de.unknownreality.dataframe.io.ReaderBuilder;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by Alex on 08.06.2016.
 */
public class DataFrameLoader {


    private DataFrameLoader() {
    }

    public static DataFrame fromCSV(File file, char separator, String headerPrefix){
        return load(file, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());

    }

    public static DataFrame fromCSV(String content, char separator, String headerPrefix){
        return load(content, CSVReaderBuilder.create().
                withHeader(true).withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }

    public static DataFrame fromCSV(String resource, ClassLoader classLoader, char separator, String headerPrefix){
        return load(resource, classLoader,
                CSVReaderBuilder.create().
                        withHeader(true).
                        withSeparator(separator).
                        withHeaderPrefix(headerPrefix).build());

    }

    public static DataFrame fromCSV(URL url, char separator, String headerPrefix){
        return load(url, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());

    }

    public static DataFrame fromCSV(byte[] bytes, char separator, String headerPrefix){
        return load(bytes, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());

    }

    public static DataFrame fromCSV(InputStream is, char separator, String headerPrefix){
        return load(is, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }


    public static DataFrame fromCSV(File file, char separator, boolean header){
        return load(file, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    public static DataFrame fromCSV(String content, char separator, boolean header){
        return load(content, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());
    }

    public static DataFrame fromCSV(String resource, ClassLoader classLoader, char separator, boolean header) {
        return load(resource, classLoader, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    public static DataFrame fromCSV(URL url, char separator, boolean header) {
        return load(url, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    public static DataFrame fromCSV(byte[] bytes, char separator, boolean header){
        return load(bytes, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    public static DataFrame fromCSV(InputStream is, char separator, boolean header){
        return load(is, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());
    }


    public static DataFrame load(File file, ReadFormat readFormat){
        return load(file, readFormat.getReaderBuilder().build());

    }

    public static DataFrame load(String content, ReadFormat readFormat){
        return load(content, readFormat.getReaderBuilder().build());
    }

    public static DataFrame load(String resource, ClassLoader classLoader, ReadFormat readFormat){
        return load(resource, classLoader, readFormat.getReaderBuilder().build());

    }

    public static DataFrame load(URL url, ReadFormat readFormat){
        return load(url, readFormat.getReaderBuilder().build());

    }

    public static DataFrame load(byte[] bytes, ReadFormat readFormat){
        return load(bytes, readFormat.getReaderBuilder().build());

    }

    public static DataFrame load(InputStream is, ReadFormat readFormat){
        return load(is, readFormat.getReaderBuilder().build());
    }


    public static DataFrame load(File file, DataReader reader){

        return load(reader.load(file));

    }

    public static DataFrame load(String content, DataReader reader){
        return load(reader.load(content));
    }

    public static DataFrame load(String resource, ClassLoader classLoader, DataReader reader){
        return load(reader.load(resource, classLoader));

    }

    public static DataFrame load(URL url, DataReader reader){
        return load(reader.load(url));

    }

    public static DataFrame load(byte[] bytes, DataReader reader){
        return load(reader.load(bytes));

    }

    public static DataFrame load(InputStream is, DataReader reader){
        return load(reader.load(is));
    }

    public static DataFrame load(DataIterator<?> dataIterator) {
        return DataFrameConverter.fromDataIterator(dataIterator, FilterPredicate.EMPTY_FILTER);
    }

    public static DataFrame load(DataIterator<?> dataIterator, FilterPredicate predicate) {
        return DataFrameConverter.fromDataIterator(dataIterator, predicate);
    }


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
        DataReader<?, ?> reader = getDataReader(dataFrameMeta);
        DataIterator<?> dataIterator = reader.load(file);
        return DataFrameConverter.fromDataIterator(dataIterator, dataFrameMeta.getColumnInformation(), filterPredicate);
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
        DataReader<?, ?> reader = getDataReader(dataFrameMeta);
        DataIterator<?> dataIterator = reader.load(path, classLoader);;
        return DataFrameConverter.fromDataIterator(dataIterator, dataFrameMeta.getColumnInformation(), filterPredicate);
    }

    private static DataReader<?, ?> getDataReader(DataFrameMeta meta) throws DataFrameException {
        ReadFormat readFormat;
        try {
            readFormat = meta.getReadFormatClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameException("error creating readformat instance", e);
        }
        ReaderBuilder readerBuilder;
        try {
            readerBuilder = readFormat.getReaderBuilder();
        } catch (Exception e) {
            throw new DataFrameException("error creating readerBuilder instance", e);
        }
        DataReader<?, ?> reader;
        try {
            reader = readerBuilder.loadSettings(meta.getAttributes()).build();
        } catch (Exception e) {
            throw new DataFrameException("error loading readerBuilder attributes", e);
        }

        return reader;
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


}
