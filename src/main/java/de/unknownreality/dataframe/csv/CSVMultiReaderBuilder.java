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

package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.MultiIterator;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVMultiReaderBuilder {
    private final File[] files;
    private Character separator = '\t';
    private String headerPrefix = "#";
    private boolean containsHeader = true;

    private CSVMultiReaderBuilder(File[] files) {
        this.files = files;
    }

    /**
     * Creates a multi csv reader from an array of input files
     *
     * @param files array of input files
     * @return multi csv iterator for specified input files
     */
    public static CSVMultiReaderBuilder create(File[] files) {
        return new CSVMultiReaderBuilder(files);
    }


    /**
     * Sets the csv column separator char
     * <p>default: <tt>'\t'</tt></p>
     *
     * @param separator csv column separator
     * @return <tt>self</tt> for method chaining
     */
    public CSVMultiReaderBuilder withSeparator(Character separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Specifies whether the files container a header line
     * <p>default: <tt>true</tt></p>
     *
     * @param containsHeader header line parameter
     * @return <tt>self</tt> for method chaining
     */
    public CSVMultiReaderBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }

    /**
     * Sets the header prefix. Only important if header line exists.
     * <p>default:<tt>#</tt></p>
     *
     * @param headerPrefix header line prefix
     * @return <tt>self</tt> for method chaining
     */
    public CSVMultiReaderBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    /**
     * Builds the csv multi iterator using the provided parameters.
     *
     * @return multi csv iterator
     */
    public MultiIterator<CSVRow> build() {
        CSVReader[] readers = new CSVReader[files.length];
        for (int i = 0; i < readers.length; i++) {
            readers[i] = CSVReaderBuilder.create()
                    .containsHeader(containsHeader)
                    .withHeaderPrefix(headerPrefix)
                    .withSeparator(separator)
                    .load(files[i]);
        }
        return MultiIterator.create(readers, CSVRow.class);
    }
}
