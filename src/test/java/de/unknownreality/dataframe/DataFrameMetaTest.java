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

import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.csv.CSVFormat;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.io.ReadFormat;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameMetaTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testMetaWriter() throws  Exception{
        DataFrame res = DataFrame.fromCSV("loader_test.csv", DataFrameLoaderTest.class.getClassLoader(), ';', false);
        res.renameColumn("V1","A");
        res.renameColumn("V2","B");
        res.renameColumn("V3","C");
        File tmpFile = File.createTempFile("dataframe", ".csv");
        res.writeCSV(tmpFile,';', true);
        DataFrameMeta meta = DataFrameMetaReader.read(new File(tmpFile.getAbsolutePath()+".dfm"));
        Assert.assertEquals("", meta.getAttributes().get("headerPrefix"));
        Assert.assertEquals(";", meta.getAttributes().get("separator"));
        Assert.assertEquals("false", meta.getAttributes().get("gzip"));
        Assert.assertEquals("true", meta.getAttributes().get("containsHeader"));
        Assert.assertEquals(5, meta.getSize());

        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assert.assertEquals(CSVFormat.class, readFormat.getClass());


        Assert.assertEquals(IntegerColumn.class, meta.getColumns().get("A"));
        Assert.assertEquals(DoubleColumn.class, meta.getColumns().get("B"));
        Assert.assertEquals(StringColumn.class, meta.getColumns().get("C"));

        tmpFile.delete();

    }

    @Test
    public void testMetaReader() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/loader_test.csv.meta"));
        Assert.assertEquals("#", meta.getAttributes().get("headerPrefix"));
        Assert.assertEquals(";", meta.getAttributes().get("separator"));
        Assert.assertEquals("false", meta.getAttributes().get("gzip"));
        Assert.assertEquals("false", meta.getAttributes().get("containsHeader"));

        Assert.assertEquals(5, meta.getSize());


        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assert.assertEquals(CSVFormat.class, readFormat.getClass());

        Assert.assertEquals(IntegerColumn.class, meta.getColumns().get("id"));
        Assert.assertEquals(DoubleColumn.class, meta.getColumns().get("value"));
        Assert.assertEquals(StringColumn.class, meta.getColumns().get("description"));

    }


    @Test
    public void testMetaLegacyReader() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/legacy_meta.dfm"));
        Assert.assertEquals("#", meta.getAttributes().get("headerPrefix"));
        Assert.assertEquals("\t", meta.getAttributes().get("separator"));
        Assert.assertEquals("true", meta.getAttributes().get("gzip"));
        Assert.assertEquals("false", meta.getAttributes().get("containsHeader"));

        Assert.assertEquals(-1, meta.getSize());


        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assert.assertEquals(CSVFormat.class, readFormat.getClass());

        CSVReaderBuilder csvReaderBuilder = (CSVReaderBuilder) readFormat.getReaderBuilder();
        csvReaderBuilder.loadSettings(meta.getAttributes());


       /* Assert.assertEquals("#", csvReaderBuilder.getHeaderPrefix());
        Assert.assertEquals(new Character('\t'), csvReaderBuilder.getSeparator());
        Assert.assertEquals(false, csvReaderBuilder.isContainsHeader());
        Assert.assertEquals(3,meta.getMetaColumns().size());*/

        Assert.assertEquals(IntegerColumn.class, meta.getColumns().get("id"));
        Assert.assertEquals(DoubleColumn.class, meta.getColumns().get("value"));
        Assert.assertEquals(StringColumn.class, meta.getColumns().get("description"));

    }

}
