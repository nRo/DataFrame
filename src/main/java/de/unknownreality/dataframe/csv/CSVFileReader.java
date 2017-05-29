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

import de.unknownreality.dataframe.common.GZipUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVFileReader extends CSVReader {
    private static final Logger log = LoggerFactory.getLogger(CSVFileReader.class);
    private final File file;
    private final boolean gzipped;
    private int skip = 0;

    /**
     * Creates a CSVFileReader
     *
     * @param file           file to be read
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVFileReader(File file, Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        super(separator, containsHeader, headerPrefix, ignorePrefixes);
        this.file = file;
        gzipped = GZipUtil.isGzipped(file);
        initHeader();
        if (containsHeader) {
            skip++;
        }
    }

    /**
     * Returns a {@link CSVIterator} from this csv file reader
     *
     * @return csv row iterator
     */
    @Override
    public CSVIterator iterator() {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            if (gzipped) {
                try {
                    inputStream = new GZIPInputStream(inputStream);
                } catch (IOException e) {
                    log.error("error creating gzip input stream", e);
                }
            }
            return new CSVIterator(inputStream, getHeader(), getSeparator(), getIgnorePrefixes(), skip);
        } catch (FileNotFoundException e) {
            throw new CSVRuntimeException(String.format("file not found: %s",file.getAbsolutePath()));
        }
    }
}
