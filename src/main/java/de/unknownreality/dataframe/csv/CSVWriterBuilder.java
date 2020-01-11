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

import de.unknownreality.dataframe.io.WriterBuilder;

/**
 * Created by Alex on 17.06.2017.
 */
public class CSVWriterBuilder implements WriterBuilder<CSVWriter> {
    private char separator = '\t';
    private String headerPrefix = "";
    private boolean containsHeader = true;
    private boolean gzip = false;
    private boolean quoteStrings = false;
    private CSVWriterBuilder(){}

    public static CSVWriterBuilder create(){
        return new CSVWriterBuilder();
    }


    public CSVWriterBuilder withSeparator(char separator){
        this.separator = separator;
        return this;
    }

    public CSVWriterBuilder withHeaderPrefix(String headerPrefix){
        this.headerPrefix = headerPrefix;
        return this;
    }

    public CSVWriterBuilder withHeader(boolean header){
        this.containsHeader = header;
        return this;
    }

    @Deprecated
    public CSVWriterBuilder containsHeader(boolean header){
        this.containsHeader = header;
        return this;
    }

    public CSVWriterBuilder quoteStrings(boolean quoteStrings) {
        this.quoteStrings = quoteStrings;
        return this;
    }

    public CSVWriterBuilder useGzip(boolean gzip){
        this.gzip = gzip;
        return this;
    }



    @Override
    public CSVWriter build() {
        CSVSettings settings = new CSVSettings();
        settings.setSeparator(separator);
        settings.setHeaderPrefix(headerPrefix);
        settings.setContainsHeader(containsHeader);
        settings.setGzip(gzip);
        settings.setQuoteStrings(quoteStrings);
        return new CSVWriter(settings);
    }
}
