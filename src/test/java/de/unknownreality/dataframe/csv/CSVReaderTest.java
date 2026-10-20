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

package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVReaderTest {
    @Test
    public void testReader() throws IOException {
        String testCSV = "#A\tB\tC\n1\tX\t3\n1\tX\t3\n";
        CSVReader reader = CSVFormat.createReader()
                .withHeaderPrefix("#")
                .withHeader(true)
                .withSeparator('\t').build();

        CSVIterator csvRows = reader.load(testCSV);
        Assertions.assertEquals("A", csvRows.getColumnsInformation().get(0).getName());
        Assertions.assertEquals("B", csvRows.getColumnsInformation().get(1).getName());
        Assertions.assertEquals("C", csvRows.getColumnsInformation().get(2).getName());

        for (CSVRow row : csvRows) {
            Assertions.assertEquals("1", row.get(0));
            Assertions.assertEquals("X", row.get(1));
            Assertions.assertEquals("3", row.get(2));

            Assertions.assertEquals("1", row.get("A"));
            Assertions.assertEquals("X", row.get("B"));
            Assertions.assertEquals("3", row.get("C"));
        }
        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assertions.assertEquals((Integer)1, row.getInteger(0));
            Assertions.assertEquals("X", row.get(1));
            Assertions.assertEquals((Integer)3, row.getInteger(2));
            Assertions.assertEquals((Integer)1, row.getInteger("A"));
            Assertions.assertEquals("X", row.get("B"));
            Assertions.assertEquals((Integer)3, row.getInteger("C"));
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
        Assertions.assertEquals(2,csvRows.getColumnsInformation().size());
        Assertions.assertEquals("A", csvRows.getColumnsInformation().get(0).getName());
        Assertions.assertEquals("B", csvRows.getColumnsInformation().get(1).getName());

        for (CSVRow row : csvRows) {
            Assertions.assertEquals("1", row.get(0));
            Assertions.assertEquals("X", row.get(1));

            Assertions.assertEquals("1", row.get("A"));
            Assertions.assertEquals("X", row.get("B"));
        }
        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assertions.assertEquals((Integer)1, row.getInteger(0));
            Assertions.assertEquals("X", row.get(1));
            Assertions.assertEquals((Integer)1, row.getInteger("A"));
            Assertions.assertEquals("X", row.get("B"));
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
        Assertions.assertEquals(2,csvRows.getColumnsInformation().size());
        Assertions.assertEquals("B", csvRows.getColumnsInformation().get(0).getName());
        Assertions.assertEquals("C", csvRows.getColumnsInformation().get(1).getName());

        for (CSVRow row : csvRows) {
            Assertions.assertEquals("X", row.get(0));
            Assertions.assertEquals("3", row.get(1));
            Assertions.assertEquals("X", row.get("B"));
            Assertions.assertEquals("3", row.get("C"));

        }

        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assertions.assertEquals("X", row.get(0));
            Assertions.assertEquals((Integer)3, row.getInteger(1));
            Assertions.assertEquals("X", row.get("B"));
            Assertions.assertEquals((Integer)3, row.getInteger("C"));
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
        Assertions.assertEquals(2,csvRows.getColumnsInformation().size());
        Assertions.assertEquals("A", csvRows.getColumnsInformation().get(0).getName());
        Assertions.assertEquals("C", csvRows.getColumnsInformation().get(1).getName());

        for (CSVRow row : csvRows) {
            Assertions.assertEquals("1", row.get(0));
            Assertions.assertEquals("3", row.get(1));
            Assertions.assertEquals("1", row.get("A"));
            Assertions.assertEquals("3", row.get("C"));
        }

        DataFrame dataFrame = DataFrame.load(testCSV,reader);
        for (DataRow row : dataFrame) {
            Assertions.assertEquals((Integer)1, row.getInteger(0));
            Assertions.assertEquals((Integer)3, row.getInteger(1));
            Assertions.assertEquals((Integer)1, row.getInteger("A"));
            Assertions.assertEquals((Integer)3, row.getInteger("C"));
        }
    }


}
