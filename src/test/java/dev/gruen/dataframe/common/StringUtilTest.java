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

package dev.gruen.dataframe.common;

import dev.gruen.dataframe.DataFrameException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Created by Alex on 28.06.2017.
 */
public class StringUtilTest {
    @Test
    public void test() throws DataFrameException {
        Assertions.assertEquals("\"test\"", StringUtil.putInQuotes("test", '"', '\\'));
        Assertions.assertEquals("\"te\\\"st\"", StringUtil.putInQuotes("te\"st", '"', '\\'));
        StringSplitter stringSplitter = StringSplitter.create();
        String splitTest1 = "testA,testB,testC";
        String[] result = new String[3];
        StringSplitter.create().splitQuoted(splitTest1, ',', result);
        Assertions.assertArrayEquals(new String[]{"testA", "testB", "testC"}, result);

        Assertions.assertArrayEquals(
                new String[]{"testA", "testB", "testC"}
                , stringSplitter.splitQuoted("'testA','testB','testC'", ','));

        Assertions.assertArrayEquals(
                new String[]{"testA", "testB", "testC"}
                , stringSplitter.splitQuoted("\"testA\",\"testB\",\"testC\"", ','));

        Assertions.assertArrayEquals(
                new String[]{"testA,testB","testC"}
                , stringSplitter.splitQuoted("'testA,testB',testC",','));



        Assertions.assertArrayEquals(
                new String[]{"testA","testB,testC"}
                , stringSplitter.splitQuoted("testA,\"testB,testC\"",','));

        Assertions.assertArrayEquals(
                new String[]{"testA"}
                , stringSplitter.splitQuoted("testA",','));

        Assertions.assertArrayEquals(
                new String[]{"testA,testB"}
                , stringSplitter.splitQuoted("'testA,testB'",','));

        Assertions.assertArrayEquals(
                new String[]{"testB,testC"}
                , stringSplitter.splitQuoted("\"testB,testC\"",','));

        Assertions.assertArrayEquals(
                new String[]{}
                , stringSplitter.splitQuoted("",','));

        Assertions.assertArrayEquals(
                new String[]{""}
                , stringSplitter.splitQuoted("\"\"",','));

        Assertions.assertArrayEquals(
                new String[]{""}
                , stringSplitter.splitQuoted("''",','));


        Assertions.assertArrayEquals(
                new String[]{"testA,testB","testC"}
                , stringSplitter.splitQuoted("testA\\,testB,testC",','));

        Assertions.assertArrayEquals(
                new String[]{"test\"A\"","testB","testC"}
                , stringSplitter.splitQuoted("test\"A\",testB,testC",','));

        Assertions.assertArrayEquals(
                new String[]{"test\"A","testB","testC"}
                , stringSplitter.splitQuoted("test\"A,testB,testC",','));
        Assertions.assertArrayEquals(
                new String[]{"\'testA\'","testB","testC"}
                , stringSplitter.splitQuoted("\\'testA\\',testB,testC",','));

        Assertions.assertArrayEquals(
                new String[]{"testA","testB,testC","testD"}
                , stringSplitter.splitQuoted("testA,\'testB,testC\',testD",','));

        stringSplitter.setDetectSingleQuotes(false);
        Assertions.assertArrayEquals(
                new String[]{"testA","'testB","testC'","testD"}
                , stringSplitter.splitQuoted("testA,\'testB,testC\',testD", ','));


        Assertions.assertArrayEquals(
                new String[]{"testA", "testB,testC", "testD"}
                , stringSplitter.splitQuoted("testA,\"testB,testC\",testD", ','));
        stringSplitter.setDetectQuotes(false);
        Assertions.assertArrayEquals(
                new String[]{"testA", "\"testB", "testC\"", "testD"}
                , stringSplitter.splitQuoted("testA,\"testB,testC\",testD", ','));


        stringSplitter.setDetectQuotes(true);
        stringSplitter.setEscapeChar('\"');
        Assertions.assertArrayEquals(
                new String[]{"testA", "\"testB,testC\"", "testD"}
                , stringSplitter.splitQuoted("testA,\"\"\"testB,testC\"\"\",testD", ','));

    }


}
