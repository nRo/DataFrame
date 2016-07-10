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

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVWriterBuilder {
    private char separator = '\t';
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private boolean gzip = false;

    /**
     * Creates a csv writer builder instance
     *
     * @return csv writer
     */
    public static CSVWriterBuilder create() {
        return new CSVWriterBuilder();
    }


    /**
     * Sets the csv column separator char used by the resulting writer
     * <p>default: <tt>'\t'</tt></p>
     *
     * @param separator csv column separator
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder withSeparator(char separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Specifies whether the resulting writer appends a header line
     * <p>default: <tt>true</tt></p>
     *
     * @param containsHeader header line parameter
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }


    /**
     * Sets the header prefix used by the resulting reader. Only important if header line exists.
     * <p>default:<tt>#</tt></p>
     *
     * @param headerPrefix header line prefix
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    /**
     * Specifies whether the resulting writer uses gzip
     * <p>default: <tt>false</tt></p>
     *
     * @param gzip header line parameter
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder useGzip(boolean gzip) {
        this.gzip = gzip;
        return this;
    }


    public char getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }

    public boolean isGzip() {
        return gzip;
    }

    /**
     * Creates a csv writer using the specified settings
     *
     * @return csv writer
     */
    public CSVWriter build() {
        return new CSVWriter(getSeparator(), isContainsHeader(), getHeaderPrefix(), isGzip());
    }


}
