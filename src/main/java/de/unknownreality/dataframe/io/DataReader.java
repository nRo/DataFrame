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

package de.unknownreality.dataframe.io;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Row;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 17.06.2017.
 */
public abstract class DataReader<R extends Row<?, ?>, C extends DataIterator<R>> {

    public Charset getCharset() {
        return Charset.defaultCharset();
    }

    public C load(File file) {
        try {
            if (GZipUtil.isGzipped(file)) {
                InputStream is = new GZIPInputStream(new FileInputStream(file));
                return load(is);
            }
            return load(new InputStreamReader(new FileInputStream(file), getCharset()));
        } catch (IOException e) {
            throw new DataFrameRuntimeException(String.format("error loading file '%s'", file.getAbsolutePath()), e);
        }
    }

    public C load(String content) {
        StringReader reader = new StringReader(content);
        return load(reader);
    }

    public C load(String resource, ClassLoader classLoader) {
        InputStream is = classLoader.getResourceAsStream(resource);
        return load(is);
    }

    public C load(URL url) {
        InputStream is;
        try {
            is = url.openStream();
        } catch (IOException e) {
            throw new DataFrameRuntimeException(String.format("error opening url stream '%s'", url), e);
        }
        return load(is);
    }

    public C load(byte[] bytes) {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        return load(is);
    }

    public C load(InputStream is) {
        InputStreamReader reader = new InputStreamReader(is);
        return load(reader);
    }

    public abstract C load(Reader reader);
}
