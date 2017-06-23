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

package de.unknownreality.dataframe.io;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 02.06.2016.
 */
public class GZipUtil {
    private static final Logger log = LoggerFactory.getLogger(GZipUtil.class);

    /**
     * Returns <tt>true</tt> if the specified file is gzipped
     *
     * @param file file to test
     * @return <tt>true</tt> if file is gzipped
     */
    public static boolean isGzipped(File file) {
        try {
            return isGzipped(new FileInputStream(file));
        } catch (Exception e) {
            log.error("error opening file", e);
        }
        return false;
    }

    /**
     * Returns <tt>true</tt> if specified {@link InputStream} is gzipped
     *
     * @param is Input stream to test
     * @return <tt>true</tt> if input stream is gzipped
     */
    public static boolean isGzipped(InputStream is) {
        if (!is.markSupported()) {
            is = new BufferedInputStream(is);
        }
        is.mark(2);
        int m;
        try {
            m = is.read() & 0xff | ((is.read() << 8) & 0xff00);
            is.reset();
        } catch (IOException e) {
            return false;
        }
        return m == GZIPInputStream.GZIP_MAGIC;
    }
}
