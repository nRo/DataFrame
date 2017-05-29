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

package de.unknownreality.dataframe.common.reader;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Alex on 19.05.2017.
 */
public abstract class BufferedStreamIterator<R extends Row> implements Iterator<R> {
    private static final Logger log = LoggerFactory.getLogger(BufferedStreamIterator.class);
    private final BufferedReader reader;
    private R next;


    public BufferedStreamIterator(InputStream stream) {
        if (stream == null) {
            throw new DataFrameRuntimeException("input stream is null");
        }
        this.reader = new BufferedReader(new InputStreamReader(stream));
    }

    protected void loadNext(){
        next = getNext();
    }

    protected String getLine() throws IOException {
        return reader.readLine();
    }

    /**
     * skips rows
     *
     * @param rows number of rows to be skipped
     */
    public void skip(int rows) {
        for (int i = 0; i < rows; i++) {
            try {
                getLine();
            } catch (IOException e) {
                log.error("error reading file:{}", e);
                close();
            }
        }
    }

    /**
     * closes this iterator
     */
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            log.error("error closing stream", e);
        }
    }


    /**
     * Reads the csv input stream and returns a row
     *
     * @return next row
     */
    protected abstract R getNext();

    /**
     * Returns true if last row is not reached yet
     *
     * @return true if next row exists
     */
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next row.
     * If last row is reached this iterator closes automatically.
     *
     * @return next row
     */
    public R next() {
        if(next == null)    {
            throw new NoSuchElementException("no next element found");
        }
        R row = next;
        next = getNext();
        if (next == null) {
            close();
        }
        return row;
    }

    /**
     * Remove is not supported
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove not supported by this iterator");
    }

}
