/*
 *
 *  * Copyright (c) 2019-2026 Alexander Grün
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

import java.io.*;
import java.nio.CharBuffer;

/**
 * Created by Alex on 23.06.2017.
 */
public class DataFrameWriter {
    /**
     * Defines whether meta files should be written per default.
     * At the moment, meta files are written automatically if the {@link DataWriter}
     * used to write the {@link DataFrame} has a matching {@link de.unknownreality.dataframe.io.ReadFormat} ({@link DataWriter#getReadFormat()}).
     */
    public final static boolean DEFAULT_WRITE_META = true;

    /**
     * Defines the default format used to print dataframes.
     * The tab separated file format is used per default.
     */
    public final  static WriteFormat DEFAULT_PRINT_FORMAT = FileFormat.Print;
    /**
     * Defines the default format used to write dataframes.
     * The tab separated file format is used per default.
     */
    public final static WriteFormat DEFAULT_WRITE_FORMAT = FileFormat.TSV;

    /**
     * Writes a dataframe to a file using a specified {@link DataWriter}.
     * If there is a matching {@link de.unknownreality.dataframe.io.DataReader} for the {@link DataWriter}, a meta file is written automatically
     *
     * @param file       target file
     * @param dataFrame  input dataframe
     * @param dataWriter data writer used to write the dataframe
     */
    public static void write(File file, DataFrame dataFrame, DataWriter dataWriter) {
        write(file, dataFrame, dataWriter, DEFAULT_WRITE_META);
    }


    /**
     * Writes a dataframe to a file using a specified {@link DataWriter}.
     * If there is a matching {@link de.unknownreality.dataframe.io.ReadFormat} for the {@link DataWriter}, a meta file is written if specified.
     *
     * @param file          target file
     * @param dataFrame     input dataframe
     * @param writeMetaFile defines whether a meta file should be created
     * @param dataWriter    data writer used to write the dataframe
     */
    public static void write(File file, DataFrame dataFrame, DataWriter dataWriter, boolean writeMetaFile) {
        dataWriter.write(file, dataFrame);
        if (writeMetaFile && dataWriter.getReadFormat() != null) {
            writeMetaFile(file, dataFrame, dataWriter);
        }
    }

    /**
     * Writes a dataframe to a {@link Writer} using a specified {@link DataWriter}.
     *
     * @param writer     target writer
     * @param dataFrame  input dataframe
     * @param dataWriter data writer used to write the dataframe
     */
    public static void write(Writer writer, DataFrame dataFrame, DataWriter dataWriter) {
        dataWriter.write(writer, dataFrame);
    }

    /**
     * Writes a dataframe to a {@link OutputStream} using a specified {@link DataWriter}.
     *
     * @param outputStream target OutputStream
     * @param dataFrame    input dataframe
     * @param dataWriter   data writer used to write the dataframe
     */
    public static void write(OutputStream outputStream, DataFrame dataFrame, DataWriter dataWriter) {
        dataWriter.write(outputStream, dataFrame);
    }

    /**
     * Writes a dataframe to a file using a specified {@link WriteFormat}.
     * If there is a matching {@link de.unknownreality.dataframe.io.ReadFormat} for the {@link WriteFormat}, a meta file is written automatically
     *
     * @param file        target file
     * @param dataFrame   input dataframe
     * @param writeFormat defines the output format used to write the dataframe
     */
    public static void write(File file, DataFrame dataFrame, WriteFormat writeFormat) {
        write(file, dataFrame, writeFormat, DEFAULT_WRITE_META);
    }


    /**
     * Writes a dataframe to a file using a specified {@link WriteFormat}.
     * If there is a matching {@link de.unknownreality.dataframe.io.ReadFormat} for the {@link WriteFormat}, a meta file is written if specified
     *
     * @param file          target file
     * @param dataFrame     input dataframe
     * @param writeFormat   defines the output format used to write the dataframe
     * @param writeMetaFile defines whether a meta file should be created
     */
    public static void write(File file, DataFrame dataFrame, WriteFormat writeFormat, boolean writeMetaFile) {
        write(file, dataFrame, writeFormat.getWriterBuilder().build(), writeMetaFile);
    }

    /**
     * Writes a dataframe to a {@link Writer} using a specified {@link WriteFormat}.
     *
     * @param writer      target writer
     * @param dataFrame   input dataframe
     * @param writeFormat data writer used to write the dataframe
     */
    public static void write(Writer writer, DataFrame dataFrame, WriteFormat writeFormat) {
        write(writer, dataFrame, writeFormat.getWriterBuilder().build());
    }

    /**
     * Writes a dataframe to a {@link OutputStream} using a specified {@link WriteFormat}.
     *
     * @param outputStream target OutputStream
     * @param dataFrame    input dataframe
     * @param writeFormat  data writer used to write the dataframe
     */
    public static void write(OutputStream outputStream, DataFrame dataFrame, WriteFormat writeFormat) {
        write(outputStream, dataFrame, writeFormat.getWriterBuilder().build());
    }

    /**
     * Writes a dataframe to a file using the default write format ({@link #DEFAULT_WRITE_FORMAT}).
     * A meta file is written automatically.
     *
     * @param file      target file
     * @param dataFrame input dataframe
     */
    public static void write(File file, DataFrame dataFrame) {
        write(file, dataFrame, DEFAULT_WRITE_FORMAT, DEFAULT_WRITE_META);
    }

    /**
     * Writes a dataframe to a file using the default write format ({@link #DEFAULT_WRITE_FORMAT}).
     * A meta file is written if specified.
     *
     * @param file          target file
     * @param dataFrame     input dataframe
     * @param writeMetaFile defines whether a meta file should be created
     */
    public static void write(File file, DataFrame dataFrame, boolean writeMetaFile) {
        write(file, dataFrame, DEFAULT_WRITE_FORMAT, writeMetaFile);
    }

    /**
     * Writes a dataframe to a {@link Writer} using the default write format ({@link #DEFAULT_WRITE_FORMAT}).
     *
     * @param writer    target writer
     * @param dataFrame input dataframe
     */
    public static void write(Writer writer, DataFrame dataFrame) {
        write(writer, dataFrame, DEFAULT_WRITE_FORMAT);
    }

    /**
     * Writes a dataframe to a {@link OutputStream} using the default write format ({@link #DEFAULT_WRITE_FORMAT}).
     *
     * @param outputStream target outputStream
     * @param dataFrame    input dataframe
     */
    public static void write(OutputStream outputStream, DataFrame dataFrame) {
        write(outputStream, dataFrame, DEFAULT_WRITE_FORMAT);
    }

    /**
     * Writes a dataframe to a file using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * A header is written if specified.
     * A meta file is written automatically.
     *
     * @param file        target file
     * @param separator   separator char
     * @param writeHeader defines whether the header should be written to the file
     * @param dataFrame   input dataframe
     */
    public static void writeCSV(File file, DataFrame dataFrame, char separator, boolean writeHeader) {
        writeCSV(file, dataFrame, separator, writeHeader, DEFAULT_WRITE_META);
    }

    /**
     * Writes a dataframe to a file using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header and meta file are written if specified.
     *
     * @param file          target file
     * @param separator     separator char
     * @param writeHeader   defines whether the header should be written to the file
     * @param writeMetaFile defines whether a meta file should be written
     * @param dataFrame     input dataframe
     */
    public static void writeCSV(File file, DataFrame dataFrame, char separator, boolean writeHeader, boolean writeMetaFile) {
        write(file, dataFrame, CSVWriterBuilder.create()
                        .withHeader(writeHeader)
                        .withSeparator(separator)
                        .build(),
                writeMetaFile);
    }

    /**
     * Writes a dataframe to a {@link Writer} using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header and meta file are written if specified.
     *
     * @param writer      target writer
     * @param separator   separator char
     * @param writeHeader defines whether the header should be written to the file
     * @param dataFrame   input dataframe
     */
    public static void writeCSV(Writer writer, DataFrame dataFrame, char separator, boolean writeHeader) {
        write(writer, dataFrame, CSVWriterBuilder.create()
                .withHeader(writeHeader)
                .withSeparator(separator)
                .build());
    }

    /**
     * Writes a dataframe to a {@link OutputStream} using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header and meta file are written if specified.
     *
     * @param outputStream target OutputStream
     * @param separator    separator char
     * @param writeHeader  defines whether the header should be written to the file
     * @param dataFrame    input dataframe
     */
    public static void writeCSV(OutputStream outputStream, DataFrame dataFrame, char separator, boolean writeHeader) {
        write(outputStream, dataFrame, CSVWriterBuilder.create()
                .withHeader(writeHeader)
                .withSeparator(separator)
                .build());
    }


    /**
     * Writes a dataframe to a file using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header is written and a header prefix is added.
     * A meta file is written automatically.
     *
     * @param file         target file
     * @param separator    separator char
     * @param headerPrefix header prefix
     * @param dataFrame    input dataframe
     */
    public static void writeCSV(File file, DataFrame dataFrame, char separator, String headerPrefix) {
        writeCSV(file, dataFrame, separator, headerPrefix, DEFAULT_WRITE_META);
    }

    /**
     * Writes a dataframe to a file using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header is written and a header prefix is added.
     * A meta file is written if specified.
     *
     * @param file          target file
     * @param separator     separator char
     * @param headerPrefix  header prefix
     * @param writeMetaFile defines whether a meta file should be written
     * @param dataFrame     input dataframe
     */
    public static void writeCSV(File file, DataFrame dataFrame, char separator, String headerPrefix, boolean writeMetaFile) {
        CSVWriter csvWriter = CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix).build();

        write(file, dataFrame, csvWriter, writeMetaFile);
    }


    /**
     * Writes a dataframe to a  {@link Writer} using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header is written and a header prefix is added.
     *
     * @param writer       target writer
     * @param separator    separator char
     * @param headerPrefix header prefix
     * @param dataFrame    input dataframe
     */
    public static void writeCSV(Writer writer, DataFrame dataFrame, char separator, String headerPrefix) {
        write(writer, dataFrame, CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }

    /**
     * Writes a dataframe to a  {@link OutputStream} using the CSV file format ({@link de.unknownreality.dataframe.csv.CSVFormat}) and a specified separator.
     * Header is written and a header prefix is added.
     *
     * @param outputStream target OutputStream
     * @param separator    separator char
     * @param headerPrefix header prefix
     * @param dataFrame    input dataframe
     */
    public static void writeCSV(OutputStream outputStream, DataFrame dataFrame, char separator, String headerPrefix) {
        write(outputStream, dataFrame, CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator(separator)
                .withHeaderPrefix(headerPrefix)
                .build());
    }


    /**
     * Writes the meta file for a dataframe and {@link DataWriter} to a target file
     * @param file target file
     * @param dataFrame input dataframe
     * @param dataWriter {@link DataWriter} used to write the dataframe
     */
    public static void writeMetaFile(File file, DataFrame dataFrame, DataWriter dataWriter) {
        File metaFile = new File(file.getAbsolutePath() + "." + DataFrameMeta.META_FILE_EXTENSION);
        DataFrameMeta meta = DataFrameMeta.create(
                dataWriter.getReadFormat().getClass(),dataWriter.getMetaColumns(dataFrame), dataWriter.getSettings(dataFrame)
        );
        DataFrameMetaWriter.write(meta, metaFile);
    }


    /**
     * Prints a dataframe to {@link System#out}  using the default print format ({@link #DEFAULT_WRITE_FORMAT}).
     *
     * @param dataFrame input dataframe
     */
    public static void print(DataFrame dataFrame) {
        write(System.out, dataFrame, DEFAULT_PRINT_FORMAT);
    }

    /**
     * Prints a dataframe to {@link System#out}  using a specified {@link DataWriter}.
     *
     * @param dataWriter data writer used to print the dataframe
     * @param dataFrame  input dataframe
     */
    public static void print(DataFrame dataFrame, DataWriter dataWriter) {
        write(System.out, dataFrame, dataWriter);

    }

    /**
     * Prints a dataframe to {@link System#out}  using a specified {@link WriteFormat}.
     *
     * @param writeFormat write format used to print the dataframe
     * @param dataFrame   input dataframe
     */
    public static void print(DataFrame dataFrame, WriteFormat writeFormat) {
        write(System.out, dataFrame, writeFormat);

    }

    /**
     * Prints a dataframe to a {@link PrintStream} using the default print format ({@link #DEFAULT_PRINT_FORMAT}).
     *
     * @param dataFrame input dataframe
     * @param out       target print stream
     */
    public static void print(DataFrame dataFrame, PrintStream out) {
        write(out, dataFrame, DEFAULT_PRINT_FORMAT);
    }

    /**
     * Prints a dataframe to a {@link PrintStream} using a specified {@link DataWriter}.
     *
     * @param dataFrame  input dataframe
     * @param dataWriter data writer used to print the dataframe
     * @param out        target print stream
     */
    public static void print(DataFrame dataFrame, DataWriter dataWriter, PrintStream out) {
        write(out, dataFrame, dataWriter);
    }

    /**
     * Prints a dataframe to a {@link PrintStream} using a specified {@link WriteFormat}.
     *
     * @param dataFrame   input dataframe
     * @param writeFormat write format used to print the dataframe
     * @param out         target print stream
     */
    public static void print(DataFrame dataFrame, WriteFormat writeFormat, PrintStream out) {
        write(out, dataFrame, writeFormat);
    }

    /**
     * Prints a dataframe to an {@link Appendable} (e.g. a {@link StringBuilder}) using the default print format ({@link #DEFAULT_PRINT_FORMAT}).
     * The appendable is not closed.
     *
     * @param dataFrame input dataframe
     * @param out       target appendable
     */
    public static void print(DataFrame dataFrame, Appendable out) {
        print(dataFrame, DEFAULT_PRINT_FORMAT, out);
    }

    /**
     * Prints a dataframe to an {@link Appendable} using a specified {@link DataWriter}.
     * The appendable is not closed.
     *
     * @param dataFrame  input dataframe
     * @param dataWriter data writer used to print the dataframe
     * @param out        target appendable
     */
    public static void print(DataFrame dataFrame, DataWriter dataWriter, Appendable out) {
        Writer writer = asWriter(out);
        write(writer, dataFrame, dataWriter);
        flush(writer);
    }

    /**
     * Prints a dataframe to an {@link Appendable} using a specified {@link WriteFormat}.
     * The appendable is not closed.
     *
     * @param dataFrame   input dataframe
     * @param writeFormat write format used to print the dataframe
     * @param out         target appendable
     */
    public static void print(DataFrame dataFrame, WriteFormat writeFormat, Appendable out) {
        print(dataFrame, writeFormat.getWriterBuilder().build(), out);
    }

    private static void flush(Writer writer) {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new DataFrameRuntimeException("error flushing the print target", e);
        }
    }

    private static Writer asWriter(Appendable appendable) {
        if (appendable == null) {
            throw new DataFrameRuntimeException("print target is null");
        }
        if (appendable instanceof Writer writer) {
            return writer;
        }
        return new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                appendable.append(CharBuffer.wrap(cbuf, off, len));
            }

            @Override
            public void flush() throws IOException {
                if (appendable instanceof Flushable flushable) {
                    flushable.flush();
                }
            }

            @Override
            public void close() throws IOException {
                flush();
            }
        };
    }
}
