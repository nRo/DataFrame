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

package de.unknownreality.dataframe.common;

import de.unknownreality.dataframe.DataFrame;

import java.io.File;
import java.io.PrintStream;
import java.util.Map;

/**
 * Created by Alex on 14.03.2016.
 */
public interface DataWriter {
    /**
     * Writes a data container into a file
     *
     * @param file          target file
     * @param dataContainer container to write
     */
    void write(File file, DataContainer<? extends Header, ? extends Row> dataContainer);

    /**
     * Writes a {@link DataFrame} into a file and if specified also writes a meta file
     *
     * @param file          target file
     * @param dataFrame     data frame to write
     * @param writeMetaFile write a meta file parameter
     */
    void write(File file, DataFrame dataFrame, boolean writeMetaFile);

    /**
     * Prints a data container to {@link System#out}
     *
     * @param dataContainer data container to print
     */
    void print(DataContainer<? extends Header, ? extends Row> dataContainer);

    /**
     * Prints a data container to a {@link java.io.PrintStream}
     *
     * @param  printStream target stream
     * @param dataContainer data container to print
     */

    void print(PrintStream printStream,DataContainer<? extends Header, ? extends Row> dataContainer);

    /**
     * Returns a attributes map used by the corresponding reader builder
     *
     * @return attributes map
     * @see ReaderBuilder#loadAttributes(Map)
     */
    Map<String, String> getAttributes();
}
