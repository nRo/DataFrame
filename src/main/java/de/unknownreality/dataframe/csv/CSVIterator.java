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

import de.unknownreality.dataframe.common.StringUtil;
import de.unknownreality.dataframe.common.reader.BufferedStreamIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class CSVIterator extends BufferedStreamIterator<CSVRow> {
    private static final Logger log = LoggerFactory.getLogger(CSVIterator.class);

    private int lineNumber = 0;
    private final Character separator;
    private int cols = -1;
    private final CSVHeader header;
    private final String[] ignorePrefixes;
    /**
     * Creates a CSVIterator
     *
     * @param stream         stream of csv content
     * @param header         csv header
     * @param separator      csv column separator
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     * @param skip           number of skipped lines at the start
     */
    public CSVIterator(InputStream stream, CSVHeader header, Character separator, String[] ignorePrefixes, int skip) {
        super(stream);
        this.separator = separator;
        this.header = header;
        this.ignorePrefixes = ignorePrefixes;
        skip(skip);
        loadNext();
    }


    /**
     * Reads the csv input stream and returns a csv row
     *
     * @return next csv row
     */
    @Override
    protected CSVRow getNext() {
        try {
            lineNumber++;
            String line = getLine();
            while (line != null && "".equals(line.trim())) {
                line = getLine();
            }
            if (line == null) {
                return null;
            }
            for (String prefix : ignorePrefixes) {
                if (prefix != null && !"".equals(prefix) && line.startsWith(prefix)) {
                    return getNext();
                }
            }
            String[] values = StringUtil.splitQuoted(line, separator);
            if (cols == -1) {
                cols = values.length;
            } else {
                if (values.length != cols) {
                    throw new CSVException(String.format("unequal number of column %d != %d in line %d", values.length, cols, lineNumber));
                }
            }
            return new CSVRow(header, values, lineNumber);

        } catch (IOException e) {
            log.error("error reading file: {}:{}", lineNumber, e);
            close();
        } catch (CSVException e) {
            log.error("error parsing file: {}:{}", lineNumber, e);
            close();
        }
        return null;
    }
}