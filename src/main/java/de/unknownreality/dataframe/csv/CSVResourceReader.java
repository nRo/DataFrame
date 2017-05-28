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

import de.unknownreality.dataframe.DataFrameRuntimeException;

import java.io.InputStream;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVResourceReader extends CSVReader {
    private final String resourcePath;
    private final ClassLoader classLoader;
    private int skip = 0;

    /**
     * Creates a CSVResourceReader
     *
     * @param resourcePath   path to csv resource
     * @param classLoader    {@link ClassLoader} used to load the resource
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVResourceReader(String resourcePath, ClassLoader classLoader, Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        super(separator, containsHeader, headerPrefix, ignorePrefixes);
        this.resourcePath = resourcePath;
        this.classLoader = classLoader;
        initHeader();
        if (containsHeader) {
            skip++;
        }
    }

    /**
     * Creates a CSVResourceReader.
     * If no {@link ClassLoader} is provided. The class loader of CSVResourceReader is used.
     *
     * @param resourcePath   path to csv resource
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVResourceReader(String resourcePath, Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        this(resourcePath, CSVResourceReader.class.getClassLoader(), separator, containsHeader, headerPrefix, ignorePrefixes);
    }

    /**
     * Returns the path to csv resource
     *
     * @return csv content
     */
    public String getResourcePath() {
        return resourcePath;
    }

    /**
     * Returns a {@link CSVIterator} for this string readers
     *
     * @return csv iterator
     */
    @Override
    public CSVIterator iterator() {
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new DataFrameRuntimeException(String.format("resource not found '%s'", resourcePath));
        }
        return new CSVIterator(inputStream
                , getHeader(), getSeparator(), getIgnorePrefixes(), skip);
    }
}
