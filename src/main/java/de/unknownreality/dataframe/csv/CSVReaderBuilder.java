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

package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.io.ReaderBuilder;
import de.unknownreality.dataframe.type.DataFrameTypeManager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 17.06.2017.
 */
public class CSVReaderBuilder implements ReaderBuilder<CSVRow, CSVReader> {
    private char separator = ';';
    private String headerPrefix = "";
    private boolean containsHeader = true;
    private boolean quoteDetection = true;
    private boolean singleQuoteDetection = true;
    private final List<String> ignoreColumns = new ArrayList<>();
    private final List<String> selectColumns = new ArrayList<>();
    private final List<String> skipPrefixes = new ArrayList<>();
    private final Map<String, Class<?>> columnTypeMap = new HashMap<>();

    public static CSVReaderBuilder create() {
        return new CSVReaderBuilder();
    }

    public CSVReaderBuilder withSeparator(char separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Detect quoted values e.g. val1 "val 2" val3
     * @param quoteDetection quoteDetection
     * @return self for method chaining
     */
    public CSVReaderBuilder withQuoteDetection(boolean quoteDetection) {
        this.quoteDetection = quoteDetection;
        return this;
    }

    /**
     * Detect single quoted values e.g. val1 'val 2' val3
     * @param singleQuoteDetection singleQuoteDetection
     * @return self for method chaining
     */
    public CSVReaderBuilder withSingleQuoteDetection(boolean singleQuoteDetection) {
        this.singleQuoteDetection = singleQuoteDetection;
        return this;
    }

    public CSVReaderBuilder addSkipPrefix(String prefix){
        skipPrefixes.add(prefix);
        return this;
    }

    public CSVReaderBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }


    public CSVReaderBuilder ignoreColumns(String... cols) {
        for (String col : cols) {
            ignoreColumn(col);
        }
        return this;
    }

    public CSVReaderBuilder ignoreColumn(String col) {
        ignoreColumns.add(col);
        return this;
    }

    public CSVReaderBuilder selectColumns(String... cols) {
        for (String col : cols) {
            selectColumn(col);
        }
        return this;
    }

    public CSVReaderBuilder selectColumn(String col) {
        selectColumns.add(col);
        return this;
    }

    public CSVReaderBuilder setColumnType(String col, Class<?> type) {
        columnTypeMap.put(col, type);
        return this;
    }

    public CSVReaderBuilder withHeader(boolean header) {
        this.containsHeader = header;
        return this;
    }

    public CSVReaderBuilder containsHeader(boolean header) {
        this.containsHeader = header;
        return this;
    }




    /**
     * Creates a {@link CSVIterator} for the specified file
     *
     * @param file source file
     * @return csv reader for source file
     * @deprecated use {@link DataFrame#fromCSV} or {@link DataFrame#load} instead.
     */
    @Deprecated
    public CSVIterator load(File file){
        return build().load(file);
    }


    /**
     * Creates a {@link CSVIterator} for the specified csv content string
     *
     * @param content csv content string
     * @return csv reader for content string
     * @deprecated use {@link DataFrame#fromCSV} or {@link DataFrame#load} instead.
     */
    @Deprecated
    public CSVIterator load(String content){
        return build().load(content);
    }

    /**
     * Creates a {@link CSVIterator} for a specified resource.
     * The provided {@link ClassLoader} is used to load the resource.
     *
     * @param resourcePath path to csv resource
     * @param classLoader  {@link ClassLoader} used to load the resource
     * @return csv reader for resource
     * @deprecated use {@link DataFrame#fromCSV} or {@link DataFrame#load} instead.
     */
    @Deprecated
    public CSVIterator loadResource(String resourcePath, ClassLoader classLoader) {
        return build().load(resourcePath,classLoader);
    }

    /**
     * Creates a {@link CSVIterator} for a specified resource.
     * The {@link ClassLoader} of {@link CSVReaderBuilder} is used to load the resource.
     *
     * @param resourcePath path to csv resource
     * @return csv reader for resource
     * @deprecated use {@link DataFrame#fromCSV} or {@link DataFrame#load} instead.
     */
    @Deprecated
    public CSVIterator loadResource(String resourcePath) {
        return build().load(resourcePath,CSVReaderBuilder.class.getClassLoader());
    }

    @Override
    public CSVReader build() {
        CSVSettings settings = new CSVSettings();
        settings.setContainsHeader(containsHeader);
        settings.setHeaderPrefix(headerPrefix);
        settings.setSeparator(separator);
        settings.setSkipPrefixes(skipPrefixes);
        settings.setQuoteDetection(quoteDetection);
        settings.setSingleQuoteDetection(singleQuoteDetection);
        CSVColumnSettings csvColumnSettings = new CSVColumnSettings();
        csvColumnSettings.getColumnTypeMap().putAll(columnTypeMap);
        csvColumnSettings.getIgnoreColumns().addAll(ignoreColumns);
        csvColumnSettings.getSelectColumns().addAll(selectColumns);
        return new CSVReader(settings, csvColumnSettings);
    }

    @Override
    public ReaderBuilder<CSVRow, CSVReader> loadSettings(Map<String, String> attributes) throws Exception {
        this.separator = DataFrameTypeManager.get().parse(Character.class, attributes.get("separator"));
        this.headerPrefix = attributes.get("headerPrefix");
        this.containsHeader = DataFrameTypeManager.get().parse(Boolean.class, attributes.get("containsHeader"));
        return this;
    }


}
