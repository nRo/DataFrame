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

package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.DataFrameBuilder;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.mapping.DataMapper;

import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class CSVReader implements DataContainer<CSVHeader, CSVRow> {
    private String headerPrefix = "#";
    private final Character separator;
    private final CSVHeader header = new CSVHeader();
    private boolean containsHeader;
    private final String[] ignorePrefixes;

    /**
     * Creates a CSVIterator
     *
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    protected CSVReader(Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
        this.ignorePrefixes = ignorePrefixes;
    }

    /**
     * Returns <tt>true</tt> if this reader considers a header row
     *
     * @return <tt>true</tt> if header row exists
     */
    public boolean containsHeader() {
        return containsHeader;
    }

    /**
     * Returns the prefix for the header line used by this reader
     *
     * @return header line prefix
     */
    public String getHeaderPrefix() {
        return headerPrefix;
    }

    /**
     * Returns the csv row separator used by this reader
     *
     * @return csv row separator
     */
    public Character getSeparator() {
        return separator;
    }

    /**
     * Returns the csv header created by this reader
     *
     * @return csv header
     */
    public CSVHeader getHeader() {
        return header;
    }

    /**
     * Returns the array of prefixes for lines ignored by this reader
     *
     * @return prefixes array
     */
    public String[] getIgnorePrefixes() {
        return ignorePrefixes;
    }

    /**
     * Reads and created the csv header
     */
    public void initHeader() {
        try {
            CSVIterator iterator = iterator();
            CSVRow row = iterator.next();
            if (row == null) {
                containsHeader = false;
                return;
            }
            if (containsHeader) {
                if (!row.get(0).startsWith(headerPrefix)) {
                    throw new CSVException("invalid header prefix in first line");
                }
                String name = row.get(0);
                name = headerPrefix == null ? name : name.substring(headerPrefix.length());
                header.add(name);
                for (int i = 1; i < row.size(); i++) {
                    name = row.get(i);
                    header.add(name);
                }
            } else {
                for (int i = 0; i < row.size(); i++) {
                    header.add();
                }
                containsHeader = false;
            }
            iterator.close();

        } catch (CSVException e) {
            throw new CSVRuntimeException("error creating csv header",e);
        }
    }

    /**
     * Returns a {@link CSVIterator} for this reader
     *
     * @return csv iterator
     */
    @Override
    public abstract CSVIterator iterator();

    /**
     * Converts this reader into a {@link DataFrameBuilder}.
     *
     * @return {@link DataFrameBuilder} for this csv reader.
     */
    public DataFrameBuilder toDataFrame() {
        return DataFrameBuilder.create(this);
    }


    public <T> List<T> map(Class<T> cl) {
        return DataMapper.map(this, cl);
    }


}
