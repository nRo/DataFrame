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

package dev.gruen.dataframe;

import dev.gruen.dataframe.column.DoubleColumn;
import dev.gruen.dataframe.column.IntegerColumn;
import dev.gruen.dataframe.column.StringColumn;
import dev.gruen.dataframe.csv.CSVFormat;
import dev.gruen.dataframe.csv.CSVReaderBuilder;
import dev.gruen.dataframe.io.ReadFormat;
import dev.gruen.dataframe.meta.DataFrameMeta;
import dev.gruen.dataframe.meta.DataFrameMetaReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameMetaTest {
    @Test
    public void testMetaWriter() throws  Exception{
        DataFrame res = DataFrame.fromCSV("loader_test.csv", DataFrameLoaderTest.class.getClassLoader(), ';', false);
        res.renameColumn("V1","A");
        res.renameColumn("V2","B");
        res.renameColumn("V3","C");
        File tmpFile = File.createTempFile("dataframe", ".csv");
        res.writeCSV(tmpFile,';', true);
        DataFrameMeta meta = DataFrameMetaReader.read(new File(tmpFile.getAbsolutePath()+".dfm"));
        Assertions.assertEquals("", meta.getAttributes().get("headerPrefix"));
        Assertions.assertEquals(";", meta.getAttributes().get("separator"));
        Assertions.assertEquals("false", meta.getAttributes().get("gzip"));
        Assertions.assertEquals("true", meta.getAttributes().get("containsHeader"));
        Assertions.assertEquals(5, meta.getSize());

        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assertions.assertEquals(CSVFormat.class, readFormat.getClass());


        Assertions.assertEquals(IntegerColumn.class, meta.getColumns().get("A"));
        Assertions.assertEquals(DoubleColumn.class, meta.getColumns().get("B"));
        Assertions.assertEquals(StringColumn.class, meta.getColumns().get("C"));

        tmpFile.delete();

    }

    @Test
    public void testMetaReader() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/loader_test.csv.meta"));
        Assertions.assertEquals("#", meta.getAttributes().get("headerPrefix"));
        Assertions.assertEquals(";", meta.getAttributes().get("separator"));
        Assertions.assertEquals("false", meta.getAttributes().get("gzip"));
        Assertions.assertEquals("false", meta.getAttributes().get("containsHeader"));

        Assertions.assertEquals(5, meta.getSize());


        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assertions.assertEquals(CSVFormat.class, readFormat.getClass());

        Assertions.assertEquals(IntegerColumn.class, meta.getColumns().get("id"));
        Assertions.assertEquals(DoubleColumn.class, meta.getColumns().get("value"));
        Assertions.assertEquals(StringColumn.class, meta.getColumns().get("description"));

    }


    @Test
    public void testMetaLegacyReader() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/legacy_meta.dfm"));
        Assertions.assertEquals("#", meta.getAttributes().get("headerPrefix"));
        Assertions.assertEquals("\t", meta.getAttributes().get("separator"));
        Assertions.assertEquals("true", meta.getAttributes().get("gzip"));
        Assertions.assertEquals("false", meta.getAttributes().get("containsHeader"));

        Assertions.assertEquals(-1, meta.getSize());


        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assertions.assertEquals(CSVFormat.class, readFormat.getClass());

        CSVReaderBuilder csvReaderBuilder = (CSVReaderBuilder) readFormat.getReaderBuilder();
        csvReaderBuilder.loadSettings(meta.getAttributes());


       /* Assertions.assertEquals("#", csvReaderBuilder.getHeaderPrefix());
        Assertions.assertEquals(new Character('\t'), csvReaderBuilder.getSeparator());
        Assertions.assertEquals(false, csvReaderBuilder.isContainsHeader());
        Assertions.assertEquals(3,meta.getMetaColumns().size());*/

        Assertions.assertEquals(IntegerColumn.class, meta.getColumns().get("id"));
        Assertions.assertEquals(DoubleColumn.class, meta.getColumns().get("value"));
        Assertions.assertEquals(StringColumn.class, meta.getColumns().get("description"));

    }

    @Test
    public void testMetaReaderRemapsOldDataframePackage() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/legacy_package_meta.dfm"));
        Assertions.assertEquals(5, meta.getSize());

        ReadFormat readFormat = meta.getReadFormatClass().newInstance();
        Assertions.assertEquals(CSVFormat.class, readFormat.getClass());

        Assertions.assertEquals(IntegerColumn.class, meta.getColumns().get("id"));
        Assertions.assertEquals(DoubleColumn.class, meta.getColumns().get("value"));
        Assertions.assertEquals(StringColumn.class, meta.getColumns().get("description"));
    }

}
