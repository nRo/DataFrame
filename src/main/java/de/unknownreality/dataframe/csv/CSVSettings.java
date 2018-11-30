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

import de.unknownreality.dataframe.io.FormatSettings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Alex on 17.06.2017.
 */
public class CSVSettings implements FormatSettings{
    private char separator = '\t';
    private List<String> skipPrefixes = new ArrayList<>();
    private boolean containsHeader = true;
    private String headerPrefix = "";
    private boolean gzip = false;
    private boolean quoteStrings = false;
    private boolean quoteDetection;
    private boolean singleQuoteDetection;

    public boolean isQuoteDetection() {
        return quoteDetection;
    }

    public boolean isSingleQuoteDetection() {
        return singleQuoteDetection;
    }

    public void setQuoteDetection(boolean quoteDetection) {
        this.quoteDetection = quoteDetection;
    }

    public void setSingleQuoteDetection(boolean singleQuoteDetection) {
        this.singleQuoteDetection = singleQuoteDetection;
    }

    public boolean isGzip() {
        return gzip;
    }

    public void setGzip(boolean gzip) {
        this.gzip = gzip;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public void setHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
    }

    public void setSkipPrefixes(List<String> skipPrefixes) {
        this.skipPrefixes = skipPrefixes;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }

    public void setQuoteStrings(boolean quoteStrings) {
        this.quoteStrings = quoteStrings;
    }

    public boolean isQuoteStrings() {
        return quoteStrings;
    }

    public void setContainsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
    }

    public char getSeparator() {
        return separator;
    }

    public List<String> getSkipPrefixes() {
        return skipPrefixes;
    }
}
