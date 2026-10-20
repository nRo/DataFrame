/*
 *
 *  * Copyright (c) 2019-2026 Alexander Grün
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
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Base class for readers that create a {@link DataIterator} from different sources.
 * <p>
 * All binary sources (file, url, resource, bytes and input stream) are decoded using {@link #getCharset()},
 * which defaults to UTF-8 independent of the platform charset.
 * Gzip compressed binary sources are detected automatically.
 * If the iterator can not be created, streams opened by this reader are closed.
 * <p>
 * Created by Alex on 17.06.2017.
 */
public abstract class DataReader<R extends Row<?, ?>, C extends DataIterator<R>> {

    /**
     * Returns the charset used to decode binary input. Defaults to UTF-8.
     *
     * @return charset used by this reader
     */
    public Charset getCharset() {
        return StandardCharsets.UTF_8;
    }

    public C load(File file) {
        String description = String.format("file '%s'", file.getAbsolutePath());
        InputStream is;
        try {
            is = new FileInputStream(file);
        } catch (IOException e) {
            throw new DataFrameRuntimeException(String.format("error loading %s", description), e);
        }
        return loadStream(is, description);
    }

    public C load(String content) {
        StringReader reader = new StringReader(content);
        return load(reader);
    }

    public C load(String resource, ClassLoader classLoader) {
        if (classLoader == null) {
            throw new DataFrameRuntimeException(String.format("error loading resource '%s': class loader is null", resource));
        }
        InputStream is = classLoader.getResourceAsStream(resource);
        if (is == null) {
            throw new DataFrameRuntimeException(String.format("resource '%s' not found", resource));
        }
        return loadStream(is, String.format("resource '%s'", resource));
    }

    public C load(URL url) {
        InputStream is;
        try {
            is = url.openStream();
        } catch (IOException e) {
            throw new DataFrameRuntimeException(String.format("error opening url stream '%s'", url), e);
        }
        return loadStream(is, String.format("url '%s'", url));
    }

    public C load(byte[] bytes) {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        return loadStream(is, "byte array");
    }

    public C load(InputStream is) {
        if (is == null) {
            throw new DataFrameRuntimeException("input stream is null");
        }
        return loadStream(is, "input stream");
    }

    public abstract C load(Reader reader);

    /**
     * Creates a {@link Reader} for a binary stream.
     * Gzip compressed streams are decompressed and the bytes are decoded using {@link #getCharset()}
     *
     * @param is binary input stream
     * @return reader that decodes the stream
     * @throws IOException if the stream header can not be read
     */
    protected Reader open(InputStream is) throws IOException {
        InputStream in = is.markSupported() ? is : new BufferedInputStream(is);
        if (GZipUtil.isGzipped(in)) {
            in = new GZIPInputStream(in);
        }
        return new InputStreamReader(in, getCharset());
    }

    private C loadStream(InputStream is, String description) {
        Reader reader = null;
        try {
            reader = open(is);
            return load(reader);
        } catch (IOException e) {
            close(is);
            throw new DataFrameRuntimeException(String.format("error loading %s", description), e);
        } catch (RuntimeException | Error e) {
            close(reader != null ? reader : is);
            throw e;
        }
    }

    private static void close(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
            // the original failure is more important than a failing close
        }
    }
}
