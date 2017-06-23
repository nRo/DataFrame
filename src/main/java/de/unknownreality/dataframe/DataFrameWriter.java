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

import de.unknownreality.dataframe.csv.CSVWriter;
import de.unknownreality.dataframe.csv.CSVWriterBuilder;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.FileFormat;
import de.unknownreality.dataframe.io.WriteFormat;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaWriter;

import java.io.File;
import java.io.OutputStream;
import java.io.Writer;

/**
 * Created by Alex on 23.06.2017.
 */
public class DataFrameWriter {
    private final static boolean DEFAULT_WRITE_META = true;
    private final static WriteFormat DEFAULT_PRINT_FORMAT = FileFormat.TSV;
    private final static WriteFormat DEFAULT_WRITE_FORMAT = FileFormat.TSV;

    public static void write(File file, DataFrame dataFrame, DataWriter dataWriter) {
        dataWriter.write(file, dataFrame);
    }

    public static void write(File file, DataFrame dataFrame, DataWriter dataWriter, boolean writeMetaFile) {
        dataWriter.write(file, dataFrame);
        if(writeMetaFile && dataWriter.getReadFormat() != null){
            writeMetaFile(file, dataFrame, dataWriter);
        }
    }

    public static void write(Writer writer, DataFrame dataFrame, DataWriter dataWriter) {
        dataWriter.write(writer, dataFrame);
    }

    public static void write(OutputStream outputStream, DataFrame dataFrame, DataWriter dataWriter) {
        dataWriter.write(outputStream, dataFrame);
    }

    public static void write(File file, DataFrame dataFrame, WriteFormat writeFormat) {
        write(file, dataFrame, writeFormat, DEFAULT_WRITE_META);
    }

    public static void write(File file, DataFrame dataFrame, WriteFormat writeFormat, boolean writeMetaFile) {
        write(file, dataFrame, writeFormat.getWriterBuilder().build(), writeMetaFile);
    }

    public static void write(Writer writer, DataFrame dataFrame, WriteFormat writeFormat) {
        write(writer, dataFrame, writeFormat.getWriterBuilder().build());
    }

    public static void write(OutputStream outputStream, DataFrame dataFrame, WriteFormat writeFormat) {
        write(outputStream, dataFrame, writeFormat.getWriterBuilder().build());
    }

    public static void write(File file, DataFrame dataFrame) {
        write(file, dataFrame, DEFAULT_WRITE_FORMAT, DEFAULT_WRITE_META);
    }

    public static void write(File file, DataFrame dataFrame, boolean writeMetaFile) {
        write(file, dataFrame, DEFAULT_WRITE_FORMAT, writeMetaFile);
    }

    public static void write(Writer writer, DataFrame dataFrame) {
        write(writer, dataFrame, FileFormat.TSV);
    }

    public static void write(OutputStream outputStream, DataFrame dataFrame) {
        write(outputStream, dataFrame, DEFAULT_WRITE_FORMAT);
    }

    public static void writeCSV(File file, DataFrame dataFrame, char separator, boolean writeHeader) {
        writeCSV(file, dataFrame, separator, writeHeader, DEFAULT_WRITE_META);
    }

    public static void writeCSV(File file, DataFrame dataFrame, char separator, boolean writeHeader, boolean writeMetaFile) {
        write(file, dataFrame, CSVWriterBuilder.create()
                        .withHeader(writeHeader)
                        .withSeparator(separator)
                        .build(),
                writeMetaFile);
    }

    public static void writeCSV(Writer writer, DataFrame dataFrame, char separator, boolean writeHeader) {
        write(writer, dataFrame, CSVWriterBuilder.create()
                .withHeader(writeHeader)
                .withSeparator(separator)
                .build());
    }

    public static void writeCSV(OutputStream outputStream, DataFrame dataFrame, char separator, boolean writeHeader) {
        write(outputStream, dataFrame, CSVWriterBuilder.create()
                .withHeader(writeHeader)
                .withSeparator(separator)
                .build());
    }

    public static void writeCSV(File file, DataFrame dataFrame, char separator, String headerPrefix) {
        writeCSV(file, dataFrame, separator, headerPrefix, DEFAULT_WRITE_META);
    }

    public static void writeCSV(File file, DataFrame dataFrame, char separator, String headerPrefix, boolean writeMetaFile) {
        CSVWriter csvWriter = CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix).build();

        write(file, dataFrame, csvWriter, writeMetaFile);
    }

    public static void writeCSV(Writer writer, DataFrame dataFrame, char separator, String headerPrefix) {
        write(writer, dataFrame, CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }

    public static void writeCSV(OutputStream outputStream, DataFrame dataFrame, char separator, String headerPrefix) {
        write(outputStream, dataFrame, CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }


    public static void writeMetaFile(File file, DataFrame dataFrame, DataWriter dataWriter) {
        DataFrameMeta metaFile = DataFrameMeta.create(
                dataFrame, dataWriter.getReadFormat().getClass(), dataWriter.getSettings()
        );
        DataFrameMetaWriter.write(metaFile, file);
    }


    public static void print(DataFrame dataFrame){
        write(System.out,dataFrame, DEFAULT_PRINT_FORMAT);
    }

    public static void print(DataFrame dataFrame,DataWriter dataWriter){
        write(System.out,dataFrame, dataWriter);

    }

    public static void print(DataFrame dataFrame, WriteFormat writeFormat){
        write(System.out,dataFrame, writeFormat);

    }
}
