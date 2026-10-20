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

package dev.gruen.dataframe.csv;

import dev.gruen.dataframe.DataFrame;
import dev.gruen.dataframe.DataFrameColumn;
import dev.gruen.dataframe.common.DataContainer;
import dev.gruen.dataframe.common.Row;
import dev.gruen.dataframe.common.StringUtil;
import dev.gruen.dataframe.common.header.Header;
import dev.gruen.dataframe.io.DataWriter;
import dev.gruen.dataframe.io.FileFormat;
import dev.gruen.dataframe.io.ReadFormat;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Alex on 17.06.2017.
 */
public class CSVWriter extends DataWriter {
    private final CSVSettings settings;

    protected CSVWriter(CSVSettings settings) {
        this.settings = settings;
    }

    @Override
    public Charset getCharset() {
        if (settings.getCharset() != null) {
            return settings.getCharset();
        }
        return super.getCharset();
    }

    @Override
    public void write(OutputStream os, DataContainer<? extends Header<?>, ? extends Row<?, ?>> dataContainer) {
        if (settings.isGzip()) {
            try {
                GZIPOutputStream gzipOutputStream = new GZIPOutputStream(os);
                super.write(gzipOutputStream, dataContainer);
                gzipOutputStream.finish();
            } catch (IOException e) {
                throw new CSVRuntimeException("error writing gzip output stream", e);
            }
            return;
        }
        super.write(os, dataContainer);
    }

    @Override
    public void write(BufferedWriter bufferedWriter, DataContainer<?, ?> dataContainer) {
        try {
            writeHeader(bufferedWriter, dataContainer.getHeader());
            for (Row<?, ?> row : dataContainer) {
                writeRow(bufferedWriter, row);
            }
        } catch (Exception e) {
            throw new CSVRuntimeException("error writing csv", e);
        }
    }

    public void writeHeader(BufferedWriter bufferedWriter, Header<?> header) throws Exception {
        if (!settings.isContainsHeader()) {
            return;
        }
        if (settings.getHeaderPrefix() != null) {
            bufferedWriter.write(settings.getHeaderPrefix());
        }
        for (int i = 0; i < header.size(); i++) {
            bufferedWriter.write(header.get(i).toString());
            if (i < header.size() - 1) {
                bufferedWriter.write(settings.getSeparator());
            }
        }
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

    public void writeRow(BufferedWriter bufferedWriter, Row<?, ?> row) throws Exception {
        char separator = settings.getSeparator();
        for (int i = 0; i < row.size(); i++) {
            String s;
            if (row.isNA(i)) {
                s = settings.getNaValue();
            } else {
                s = row.toString(i);
                boolean putInQuotes = false;
                boolean escapeQuotes = false;
                char[] chars = s.toCharArray();
                for (int j = 0; j < chars.length; j++) {
                    char c = chars[j];
                    if (c == separator || c == '\'') {
                        putInQuotes = true;
                    } else if (c == '\"') {
                        putInQuotes = true;
                        escapeQuotes = true;
                    }
                }
                putInQuotes = putInQuotes
                        || (settings.isQuoteStrings() && row.getType(i).getType() == String.class);
                if (putInQuotes) {
                    if (escapeQuotes) {
                        s = StringUtil.putInQuotes(s, '\"', settings.getEscapeChar());
                    } else {
                        s = "\"" + s + "\"";
                    }
                }
            }
            bufferedWriter.write(s);
            if (i < row.size() - 1) {
                bufferedWriter.write(settings.getSeparator());
            }
        }
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

    @Override
    public void write(File file, DataContainer<? extends Header<?>, ? extends Row<?, ?>> dataContainer) {
        if (settings.isGzip()) {
            try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(file))) {
                write(new BufferedWriter(new OutputStreamWriter(outputStream, getCharset())), dataContainer);
                return;
            } catch (IOException e) {
                throw new CSVRuntimeException(String.format("error writing file '%s'", file.getAbsolutePath()), e);
            }
        }
        super.write(file, dataContainer);
    }


    @Override
    public Map<String, String> getSettings(DataFrame dataFrame) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("separator", Character.toString(settings.getSeparator()));
        attributes.put("headerPrefix", settings.getHeaderPrefix());
        attributes.put("containsHeader", Boolean.toString(settings.isContainsHeader()));
        attributes.put("gzip", Boolean.toString(settings.isGzip()));
        return attributes;
    }

    @Override
    public List<DataFrameColumn<?, ?>> getMetaColumns(DataFrame dataFrame) {
        return new ArrayList<>(dataFrame.getColumns());
    }


    @Override
    public ReadFormat<CSVRow, CSVReaderBuilder> getReadFormat() {
        return FileFormat.CSV;
    }
}
