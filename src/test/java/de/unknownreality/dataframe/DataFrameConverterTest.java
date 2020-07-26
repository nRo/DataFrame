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

package de.unknownreality.dataframe;

import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

public class DataFrameConverterTest {
    @Test
    public void testAutoDetection(){

        StringBuilder csvString = new StringBuilder();
        csvString.append("x;y;v;b\n");
        for (int i = 0; i < 1000; i++) {
            csvString.append(String.format(Locale.US, "%d;%f;%s;%b", i, i * 0.7, i + "s", true));
            csvString.append("\n");
        }

        DataFrame df = DataFrame.fromCSV(csvString.toString(), ';', true);
        Assert.assertEquals(Integer.class, df.getHeader().getValueType("x").getType());
        Assert.assertEquals(Double.class, df.getHeader().getValueType("y").getType());
        Assert.assertEquals(String.class, df.getHeader().getValueType("v").getType());
        Assert.assertEquals(Boolean.class, df.getHeader().getValueType("b").getType());


        csvString = new StringBuilder();
        csvString.append("x;y;v;b\n");
        for (int i = 0; i < 1000; i++) {
            if (i == 101) {
                csvString.append(String.format(Locale.US, "%s;%f;%s;%b", "x", i * 0.7, i + "s", true));
            } else {
                csvString.append(String.format(Locale.US, "%d;%f;%s;%b", i, i * 0.7, i + "s", true));
            }
            csvString.append("\n");
        }

        df = DataFrame.fromCSV(csvString.toString(), ';', true);
        Assert.assertEquals(String.class, df.getHeader().getValueType("x").getType());
        Assert.assertEquals(Double.class, df.getHeader().getValueType("y").getType());
        Assert.assertEquals(String.class, df.getHeader().getValueType("v").getType());
        Assert.assertEquals(Boolean.class, df.getHeader().getValueType("b").getType());
    }
}
