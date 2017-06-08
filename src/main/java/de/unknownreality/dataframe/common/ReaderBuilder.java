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

package de.unknownreality.dataframe.common;

import java.io.File;
import java.util.Map;

/**
 * Created by Alex on 07.06.2016.
 */
public interface ReaderBuilder<H extends Header, R extends Row> {
    /**
     * Loads a map of attributes.
     * Used to create readers from data frame meta files
     *
     * @param attributes map of attributes
     * @throws Exception throws an exception if any error occurs
     */
    void loadAttributes(Map<String, String> attributes) throws Exception;

    /**
     * Creates a data container from a file.
     *
     * @param f file to be read
     * @return created data container
     */
    DataContainer<H, R> fromFile(File f);

    /**
     * Creates a data container from a string
     *
     * @param content string content used to create the data container
     * @return created data container
     */
    DataContainer<H, R> fromString(String content);

    /**
     * Creates a data container from a resoruce
     * @param content resource path
     * @param classLoader class loader
     * @return created data container
     */
    DataContainer<H, R> fromResource(String content,ClassLoader classLoader);


}
