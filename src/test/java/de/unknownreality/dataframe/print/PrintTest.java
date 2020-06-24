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

package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameLoaderTest;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.type.DataFrameTypeManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;
import java.util.Iterator;

public class PrintTest {
    @Test
    public void testPrinter() {

        DataFrame df = DataFrame.fromCSV("users.csv",
                DataFrameLoaderTest.class.getClassLoader(), ';', true);
        df.addColumn(Double.class, "value", DataFrameTypeManager.createNew(), row -> Math.random());
        String corner = "o";
        String vline = "|";
        String hline = "-";
        String joint = "x";
        int width = 10;
        int cwidth = 8;
        Printer printer = PrinterBuilder.create()
                .withColumnHeaderFormatter("name", (t, h, m) -> "||" + h.toString())
                .withColumnValueFormatter("name", (t, v, m) -> v.toString().toUpperCase())
                .withHorizontalLine(hline)
                .withVerticalLine(vline)
                .withJoint(joint)
                .withCorner(corner)
                .withDefaultColumnWidth(width)
                .withDefaultMaxContentWidth(cwidth)
                .build();
        df.getRow(2).set(0, "ParkerParker");
        df.getRow(3).set(2, "GermanyGermany");
        StringWriter sw = new StringWriter();
        df.write(sw, printer);
        String[] lines = sw.toString().split("\\r?\\n");

        //content lines (+header) + inner lines + outer lines
        int expected = (df.size() + 1) + df.size() + 2;
        Assert.assertEquals(expected, lines.length);

        //cols * width + inner lines + outer lines
        int cols = df.getRow(0).size();
        int expectedRow = cols * width + cols - 1 + 2;
        Iterator<DataRow> dfIt = df.iterator();
        for (int i = 0; i < lines.length; i++) {
            Assert.assertEquals(expectedRow, lines[i].length());
            //top / bottom line
            if (i == 0 || i == lines.length - 1) {
                Assert.assertEquals(corner, String.valueOf(lines[i].charAt(0)));
                Assert.assertEquals(corner, String.valueOf(lines[i].charAt(lines[i].length() - 1)));
                for (int j = 1; j < expectedRow - 1; j++) {
                    if (j % (width + 1) == 0) {
                        Assert.assertEquals(joint, String.valueOf(lines[i].charAt(j)));
                    } else {
                        Assert.assertEquals(hline, String.valueOf(lines[i].charAt(j)));
                    }
                }
                continue;
            }


            //inner line
            if (i % 2 == 0) {
                Assert.assertEquals(joint, String.valueOf(lines[i].charAt(0)));
                Assert.assertEquals(joint, String.valueOf(lines[i].charAt(lines[i].length() - 1)));
                for (int j = 1; j < expectedRow - 1; j++) {
                    if (j % (width + 1) == 0) {
                        Assert.assertEquals(joint, String.valueOf(lines[i].charAt(j)));
                    } else {
                        Assert.assertEquals(hline, String.valueOf(lines[i].charAt(j)));
                    }
                }
                continue;
            }
            Assert.assertEquals(vline, String.valueOf(lines[i].charAt(0)));
            Assert.assertEquals(vline, String.valueOf(lines[i].charAt(lines[i].length() - 1)));
            for (int j = 1; j < expectedRow - 1; j++) {
                if (j % (width + 1) == 0) {
                    Assert.assertEquals(vline, String.valueOf(lines[i].charAt(j)));
                }
            }
            //header
            if (i == 1) {
                assertContent("||name", lines[i], width, 0);
                assertContent("#age", lines[i], width, 1);
                assertContent("#country", lines[i], width, 2);
                continue;
            }
            DataRow row = dfIt.next();
            for (int j = 0; j < row.size(); j++) {
                String c;
                if (row.get(j) instanceof Number) {
                    c = printer.getDefaultNumberFormatter().format(row.getType(j), row.get(j), cwidth);
                } else {
                    c = row.getString(j);
                }
                if (j == 0) {
                    c = c.toUpperCase();
                }
                if (j == 0 && "PARKERPARKER".equals(c)) {
                    c = "PARKERPA";
                } else if (j == 2 && "GermanyGermany".equals(c)) {
                    c = "Germa...";
                }
                assertContent(c, lines[i], width, j);
            }
        }
    }

    @Test
    public void testAutoWidth() {
        Printer printer = PrinterBuilder.create()
                .build();

        DataFrame df = DataFrame.create()
                .addStringColumn("testA")
                .addDoubleColumn("testB");
        df.append("abc", 1.5d);
        df.append("abcdefgh", 1.5d);
        df.append("abcdef", 1.5d);
        df.append("abcdefghijklmnopqrstuvwxyz", 1.5d);
        df.append("ab", 1.5d);
        StringWriter sw = new StringWriter();
        df.write(sw, printer);
        String[] lines = sw.toString().split("\\r?\\n");
        Assert.assertEquals("│abcdefg...  │1.50000000  │", lines[9]);

        printer = PrinterBuilder.create()
                .withAutoWidth("testA")
                .build();
        sw = new StringWriter();
        df.write(sw, printer);
        lines = sw.toString().split("\\r?\\n");
        Assert.assertEquals("│abcdefghijklmnopqrstuvwxyz │1.50000000  │", lines[9]);
    }

    private void assertContent(String expected, String line, int width, int col) {

        String content = line.substring(1 + col + col * width, 1 + col + (col + 1) * width).trim();
        Assert.assertEquals(expected, content);
    }
}
