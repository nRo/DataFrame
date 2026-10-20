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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.io.FileFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameWriterTest {


    @Test
    public void writerTest() throws IOException {

        DataFrame res = DataFrame.fromCSV("loader_test.csv", DataFrameWriterTest.class.getClassLoader(), ';', false);
        Assertions.assertEquals(5, res.size());
        Assertions.assertEquals(3, res.getColumns().size());

        StringWriter stringWriter = new StringWriter();
        res.write(stringWriter);
        String content = stringWriter.toString();

        DataFrame res2 = DataFrame.load(content, FileFormat.TSV);

        Assertions.assertEquals(res, res2);

        res2 = DataFrame.load(content.getBytes(), FileFormat.TSV);

        Assertions.assertEquals(res, res2);
    }



}
