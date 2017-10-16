/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe.frame;

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

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameMetaTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testMetaReader() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/legacy_meta.dfm"));
        Assert.assertEquals("#", meta.getAttributes().get("headerPrefix"));
        Assert.assertEquals("\t", meta.getAttributes().get("separator"));
        Assert.assertEquals("true", meta.getAttributes().get("gzip"));
        Assert.assertEquals("false", meta.getAttributes().get("containsHeader"));



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
