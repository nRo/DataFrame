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

package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVReaderTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testReader() throws IOException {
        String testCSV = "#A\tB\tC\n1\tX\t3\n1\tX\t3\n";
        CSVReader reader = CSVFormat.createReader()
                .withHeaderPrefix("#")
                .withHeader(true)
                .withSeparator('\t').build();

        CSVIterator csvRows = reader.load(testCSV);
        Assert.assertEquals("A", csvRows.getColumnsInformation().get(0).getName());
        Assert.assertEquals("B", csvRows.getColumnsInformation().get(1).getName());
        Assert.assertEquals("C", csvRows.getColumnsInformation().get(2).getName());

        for (CSVRow row : csvRows) {
            Assert.assertEquals("1", row.get(0));
            Assert.assertEquals("X", row.get(1));
            Assert.assertEquals("3", row.get(2));

            Assert.assertEquals("1", row.get("A"));
            Assert.assertEquals("X", row.get("B"));
            Assert.assertEquals("3", row.get("C"));
        }
        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assert.assertEquals((Integer)1, row.getInteger(0));
            Assert.assertEquals("X", row.get(1));
            Assert.assertEquals((Integer)3, row.getInteger(2));
            Assert.assertEquals((Integer)1, row.getInteger("A"));
            Assert.assertEquals("X", row.get("B"));
            Assert.assertEquals((Integer)3, row.getInteger("C"));
        }
    }

    @Test
    public void testSelect() throws IOException {
        String testCSV = "#A\tB\tC\n1\tX\t3\n1\tX\t3\n";
        CSVReader reader = CSVFormat.createReader()
                .withHeaderPrefix("#")
                .withHeader(true)
                .withSeparator('\t')
                .selectColumns("A","B")
                .build();

        CSVIterator csvRows = reader.load(testCSV);
        Assert.assertEquals(2,csvRows.getColumnsInformation().size());
        Assert.assertEquals("A", csvRows.getColumnsInformation().get(0).getName());
        Assert.assertEquals("B", csvRows.getColumnsInformation().get(1).getName());

        for (CSVRow row : csvRows) {
            Assert.assertEquals("1", row.get(0));
            Assert.assertEquals("X", row.get(1));

            Assert.assertEquals("1", row.get("A"));
            Assert.assertEquals("X", row.get("B"));
        }
        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assert.assertEquals((Integer)1, row.getInteger(0));
            Assert.assertEquals("X", row.get(1));
            Assert.assertEquals((Integer)1, row.getInteger("A"));
            Assert.assertEquals("X", row.get("B"));
        }
    }

    @Test
    public void testSkipFirst() throws IOException {
        String testCSV = "#A\tB\tC\n1\tX\t3\n1\tX\t3\n";
        CSVReader reader = CSVFormat.createReader()
                .withHeaderPrefix("#")
                .withHeader(true)
                .withSeparator('\t')
                .selectColumns("B","C")
                .build();

        CSVIterator csvRows = reader.load(testCSV);
        Assert.assertEquals(2,csvRows.getColumnsInformation().size());
        Assert.assertEquals("B", csvRows.getColumnsInformation().get(0).getName());
        Assert.assertEquals("C", csvRows.getColumnsInformation().get(1).getName());

        for (CSVRow row : csvRows) {
            Assert.assertEquals("X", row.get(0));
            Assert.assertEquals("3", row.get(1));
            Assert.assertEquals("X", row.get("B"));
            Assert.assertEquals("3", row.get("C"));

        }

        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assert.assertEquals("X", row.get(0));
            Assert.assertEquals((Integer)3, row.getInteger(1));
            Assert.assertEquals("X", row.get("B"));
            Assert.assertEquals((Integer)3, row.getInteger("C"));
        }
    }
    @Test
    public void testEmptyCols(){
        String testCSV = "#A\tB\tC\n1\t\t3\n1\tX\t3\n";
        CSVReader reader = CSVFormat.createReader()
                .withHeaderPrefix("#")
                .withHeader(true)
                .withSeparator('\t')

                .build();

        CSVIterator csvRows = reader.load(testCSV);
        DataFrame dataFrame = DataFrame.load(testCSV,reader);

    }
    @Test
    public void testSkipMid() throws IOException {
        String testCSV = "#A\tB\tC\n1\tX\t3\n1\tX\t3\n";
        CSVReader reader = CSVFormat.createReader()
                .withHeaderPrefix("#")
                .withHeader(true)
                .withSeparator('\t')
                .selectColumns("A","C")
                .build();

        CSVIterator csvRows = reader.load(testCSV);
        Assert.assertEquals(2,csvRows.getColumnsInformation().size());
        Assert.assertEquals("A", csvRows.getColumnsInformation().get(0).getName());
        Assert.assertEquals("C", csvRows.getColumnsInformation().get(1).getName());

        for (CSVRow row : csvRows) {
            Assert.assertEquals("1", row.get(0));
            Assert.assertEquals("3", row.get(1));
            Assert.assertEquals("1", row.get("A"));
            Assert.assertEquals("3", row.get("C"));
        }

        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assert.assertEquals((Integer)1, row.getInteger(0));
            Assert.assertEquals((Integer)3, row.getInteger(1));
            Assert.assertEquals((Integer)1, row.getInteger("A"));
            Assert.assertEquals((Integer)3, row.getInteger("C"));
        }
    }


}
