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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.io.*;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

/**
 * Created by Alex on 08.06.2016.
 */
public class DataFrameLoader {
    private final static ReadFormat<?, ?> DEFAULT_READ_FORMAT = FileFormat.TSV;


    private DataFrameLoader() {
    }

    /**
     * Loads a data frame from a file string using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param file input file
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(File file, char separator, String headerPrefix) {
        return load(file, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());

    }

    /**
     * Loads a data frame from a content string using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param content content string
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(String content, char separator, String headerPrefix) {
        return load(content, CSVReaderBuilder.create().
                withHeader(true).withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }

    /**
     * Loads a data frame from a resource using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param resource resource path
     * @param classLoader class loader used to find the resource
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(String resource, ClassLoader classLoader, char separator, String headerPrefix) {
        return load(resource, classLoader,
                CSVReaderBuilder.create().
                        withHeader(true).
                        withSeparator(separator).
                        withHeaderPrefix(headerPrefix).build());

    }


    /**
     * Loads a data frame from a url using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param url input url
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(URL url, char separator, String headerPrefix) {
        return load(url, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());

    }

    /**
     * Loads a data frame from a byte array using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param bytes input byte array
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(byte[] bytes, char separator, String headerPrefix) {
        return load(bytes, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());

    }

    /**
     * Loads a data frame from a {@link InputStream} using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param is input stream
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(InputStream is, char separator, String headerPrefix) {
        return load(is, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }

    /**
     * Loads a data frame from a {@link Reader} using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified.
     * If the header starts with a certain prefix it can be specified, otherwise the prefix should be set to "" or null
     * @param r input reader
     * @param  separator column separator
     * @param headerPrefix header prefix
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(Reader r, char separator, String headerPrefix) {
        return load(r, CSVReaderBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }

    /**
     * Loads a data frame from a file using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param file input file
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(File file, char separator, boolean header) {
        return load(file, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }


    /**
     * Loads a data frame from a content string using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param content content string
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(String content, char separator, boolean header) {
        return load(content, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());
    }

    /**
     * Loads a data frame from a URL array using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param resource resource path
     * @param classLoader class loader used to find the resource
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(String resource, ClassLoader classLoader, char separator, boolean header) {
        return load(resource, classLoader, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    /**
     * Loads a data frame from a URL array using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param url input url
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(URL url, char separator, boolean header) {
        return load(url, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    /**
     * Loads a data frame from a byte array using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param bytes input byte array
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(byte[] bytes, char separator, boolean header) {
        return load(bytes, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());

    }

    /**
     * Loads a data frame from a {@link InputStream} using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param is input stream
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(InputStream is, char separator, boolean header) {
        return load(is, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());
    }

    /**
     * Loads a data frame from a {@link Reader} using the CSV format ({@link de.unknownreality.dataframe.csv.CSVFormat}).
     * The column separator can be specified. If the CSV contains no header, the columns are named V1, V2,...
     * @param r input reader
     * @param  separator column separator
     * @param header specifies wether the csv contains a header or not
     * @return resulting dataframe
     */
    public static DataFrame fromCSV(Reader r, char separator, boolean header) {
        return load(r, CSVReaderBuilder.create()
                .withHeader(header)
                .withSeparator(separator)
                .build());
    }


    /**
     * Loads a data frame from a content string using the default tab separated format ({@link de.unknownreality.dataframe.csv.TSVFormat}).
     *
     * @param content content string
     * @return resulting dataframe
     */
    public static DataFrame load(String content) {
        return load(content, DEFAULT_READ_FORMAT);
    }


    /**
     * Loads a data frame from a resource using the default tab separated format ({@link de.unknownreality.dataframe.csv.TSVFormat}).
     *
     * @param resource resource path
     * @param classLoader class loader used to find the resource
     * @return resulting dataframe
     */
    public static DataFrame load(String resource, ClassLoader classLoader) {
        return load(resource, classLoader, DEFAULT_READ_FORMAT);

    }

    /**
     * Loads a data frame from a URL using the default tab separated format ({@link de.unknownreality.dataframe.csv.TSVFormat}).
     *
     * @param url input url
     * @return resulting dataframe
     */
    public static DataFrame load(URL url) {
        return load(url, DEFAULT_READ_FORMAT);

    }

    /**
     * Loads a data frame from a byte array using the default tab separated format ({@link de.unknownreality.dataframe.csv.TSVFormat}).
     *
     * @param bytes input byte array
     * @return resulting dataframe
     */
    public static DataFrame load(byte[] bytes) {
        return load(bytes, DEFAULT_READ_FORMAT);

    }

    /**
     * Loads a data frame from a {@link InputStream} using the default tab separated format ({@link de.unknownreality.dataframe.csv.TSVFormat}).
     *
     * @param is input stream
     * @return resulting dataframe
     */
    public static DataFrame load(InputStream is) {
        return load(is, DEFAULT_READ_FORMAT);
    }

    /**
     * Loads a data frame from a {@link Reader} using the default tab separated format ({@link de.unknownreality.dataframe.csv.TSVFormat}).
     *
     * @param reader input reader
     * @return resulting dataframe
     */
    public static DataFrame load(Reader reader) {
        return load(reader, DEFAULT_READ_FORMAT);
    }


    /**
     * Loads a data frame from a file using a specified {@link ReadFormat}.
     *
     * @param file       input file
     * @param readFormat read format
     * @return resulting dataframe
     */
    public static DataFrame load(File file, ReadFormat<?, ?> readFormat) {
        return load(file, readFormat.getReaderBuilder().build());

    }

    /**
     * Loads a data frame from a content String using a specified {@link ReadFormat}.
     *
     * @param content    content string
     * @param readFormat read format
     * @return resulting dataframe
     */
    public static DataFrame load(String content, ReadFormat<?, ?> readFormat) {
        return load(content, readFormat.getReaderBuilder().build());
    }


    /**
     * Loads a data frame from a resource using a specified {@link ReadFormat}
     *
     * @param resource    resource path
     * @param classLoader ClassLoader used to find the resource
     * @param readFormat  read format
     * @return resulting dataframe
     */
    public static DataFrame load(String resource, ClassLoader classLoader, ReadFormat<?, ?> readFormat) {
        return load(resource, classLoader, readFormat.getReaderBuilder().build());

    }

    /**
     * Loads a data frame from a URL array using a specified {@link ReadFormat}.
     *
     * @param url        input url
     * @param readFormat read format
     * @return resulting dataframe
     */
    public static DataFrame load(URL url, ReadFormat<?, ?> readFormat) {
        return load(url, readFormat.getReaderBuilder().build());

    }

    /**
     * Loads a data frame from a byte array using a specified {@link ReadFormat}.
     *
     * @param bytes      input byte array
     * @param readFormat read format
     * @return resulting dataframe
     */
    public static DataFrame load(byte[] bytes, ReadFormat<?, ?> readFormat) {
        return load(bytes, readFormat.getReaderBuilder().build());

    }


    /**
     * Loads a data frame from a {@link InputStream} using a specified {@link ReadFormat}.
     *
     * @param is         input stream
     * @param readFormat read format
     * @return resulting dataframe
     */
    public static DataFrame load(InputStream is, ReadFormat<?, ?> readFormat) {
        return load(is, readFormat.getReaderBuilder().build());
    }


    /**
     * Loads a data frame from a {@link Reader} using a specified {@link ReadFormat}.
     *
     * @param r          input reader
     * @param readFormat read format
     * @return resulting dataframe
     */
    public static DataFrame load(Reader r, ReadFormat<?, ?> readFormat) {
        return load(r, readFormat.getReaderBuilder().build());
    }


    /**
     * Loads a data frame from a file using a specified {@link DataReader}
     *
     * @param file   input file
     * @param reader data reader
     * @return resulting dataframe
     */
    public static DataFrame load(File file, DataReader<?, ?> reader) {

        return load(reader.load(file));

    }

    /**
     * Loads a data frame from a content String using a specified {@link DataReader}
     *
     * @param content content string
     * @param reader  data reader
     * @return resulting dataframe
     */
    public static DataFrame load(String content, DataReader<?, ?> reader) {
        return load(reader.load(content));
    }

    /**
     * Loads a data frame from a resource using a specified {@link DataReader}
     *
     * @param resource    resource path
     * @param classLoader ClassLoader used to find the resource
     * @param reader      data reader
     * @return resulting dataframe
     */
    public static DataFrame load(String resource, ClassLoader classLoader, DataReader<?, ?> reader) {
        return load(reader.load(resource, classLoader));

    }

    /**
     * Loads a data frame from a URL using a specified {@link DataReader}
     *
     * @param url    input url
     * @param reader data reader
     * @return resulting dataframe
     */
    public static DataFrame load(URL url, DataReader<?, ?> reader) {
        return load(reader.load(url));

    }

    /**
     * Loads a data frame from a byte array using a specified {@link DataReader}
     *
     * @param bytes  input byte array
     * @param reader data reader
     * @return resulting dataframe
     */
    public static DataFrame load(byte[] bytes, DataReader<?, ?> reader) {
        return load(reader.load(bytes));

    }

    /**
     * Loads a data frame from a {@link InputStream} using a specified {@link DataReader}
     *
     * @param is     input
     * @param reader data reader
     * @return resulting dataframe
     */
    public static DataFrame load(InputStream is, DataReader<?, ?> reader) {
        return load(reader.load(is));
    }

    /**
     * Loads a data frame from a {@link Reader} using a specified {@link DataReader}
     *
     * @param r      input reader
     * @param reader data reader
     * @return resulting dataframe
     */
    public static DataFrame load(Reader r, DataReader<?, ?> reader) {
        return load(reader.load(r));
    }

    /**
     * Loads a data frame from a {@link DataIterator}
     *
     * @param dataIterator data iterator
     * @return resulting dataframe
     */
    public static DataFrame load(DataIterator<?> dataIterator) {
        return load(dataIterator, FilterPredicate.EMPTY_FILTER);
    }

    /**
     * Loads a data frame from a {@link DataIterator} and filters all rows using a specified predicate
     *
     * @param dataIterator data iterator
     * @param predicate    filter predicate
     * @return resulting dataframe
     */
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
     */
    public static DataFrame load(File file, FilterPredicate filterPredicate) {
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
        if (!metaFile.exists()) {
            return load(dataFile, DEFAULT_READ_FORMAT);
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
     */
    public static DataFrame load(File file) {
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
     */

    public static DataFrame load(File file, File metaFile, FilterPredicate filterPredicate) {
        if (!file.exists()) {
            throw new DataFrameRuntimeException(String.format("file not found %s", file.getAbsolutePath()));
        }
        if (!metaFile.exists()) {
            throw new DataFrameRuntimeException(String.format("meta file not found %s", metaFile.getAbsolutePath()));
        }

        DataFrameMeta dataFrameMeta;
        try {
            dataFrameMeta = DataFrameMetaReader.read(metaFile);
        } catch (DataFrameException e) {
            throw new DataFrameRuntimeException("error loading reading meta file", e);
        }
        DataReader<?, ?> reader = getDataReader(dataFrameMeta);
        DataIterator<?> dataIterator = reader.load(file);
        return DataFrameConverter.fromDataIterator(dataIterator,dataFrameMeta.getSize(), dataFrameMeta.getColumnInformation(), filterPredicate);
    }

    /**
     * Loads a data frame from a file and the corresponding meta file.
     *
     * @param file     data frame file
     * @param metaFile data frame meta file
     * @return loaded data frame
     */
    public static DataFrame load(File file, File metaFile) {
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
     */

    public static DataFrame loadResource(String path, String metaPath, ClassLoader classLoader, FilterPredicate filterPredicate) {

        DataFrameMeta dataFrameMeta;
        try {
            dataFrameMeta = DataFrameMetaReader.read(classLoader.getResourceAsStream(metaPath));
        } catch (DataFrameException e) {
            throw new DataFrameRuntimeException("error reading meta file", e);
        }
        DataReader<?, ?> reader = getDataReader(dataFrameMeta);
        DataIterator<?> dataIterator = reader.load(path, classLoader);
        return DataFrameConverter.fromDataIterator(dataIterator, dataFrameMeta.getColumnInformation(), filterPredicate);
    }

    private static DataReader<?, ?> getDataReader(DataFrameMeta meta) {
        ReadFormat<?, ?> readFormat;
        try {
            readFormat = meta.getReadFormatClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("error creating readformat instance", e);
        }
        ReaderBuilder<?, ?> readerBuilder;
        try {
            readerBuilder = readFormat.getReaderBuilder();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("error creating readerBuilder instance", e);
        }
        DataReader<?, ?> reader;
        try {
            reader = readerBuilder.loadSettings(meta.getAttributes()).build();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("error loading readerBuilder attributes", e);
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
     */
    public static DataFrame loadResource(String path, String metaPath, ClassLoader classLoader) {
        return loadResource(path, metaPath, classLoader, FilterPredicate.EMPTY_FILTER);
    }
}
