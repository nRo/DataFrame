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

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameException;
import de.unknownreality.dataframe.DataFrameLoader;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameLoaderTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();


    @Test
    public void legacyReaderTest() throws IOException {
        DataFrame legacy = CSVReaderBuilder.create()
                .containsHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .loadResource("data_grouping.csv", DataFrameGroupingTest.class.getClassLoader())
                .toDataFrame()
                .addColumn(new IntegerColumn("ID"))
                .addColumn(new StringColumn("NAME"))
                .addColumn(new IntegerColumn("VALUE")).build();
    }
    @Test
    public void loaderTest() throws IOException {

        DataFrame res = DataFrame.fromCSV("loader_test.csv", DataFrameLoaderTest.class.getClassLoader(), ';', false);
        Assert.assertEquals(5, res.size());
        Assert.assertEquals(3, res.getColumns().size());
        CSVReader csvReader = CSVReaderBuilder.create().withSeparator(';').withHeader(false).build();
        DataFrame test = DataFrame.load("loader_test.csv",DataFrameLoaderTest.class.getClassLoader(),csvReader);
        Assert.assertEquals(res,test);

        File tmpFile = File.createTempFile("dataframe", ".csv");
        res.writeCSV(tmpFile,';', false);

        URI uri = tmpFile.toURI();
        DataFrame url = DataFrame.fromCSV(uri.toURL(),';', false);





        Assert.assertEquals(res, url);
        res.write(tmpFile);
        DataFrame file = DataFrame.load(tmpFile);
        Assert.assertEquals(file, url);

        byte[] data = Files.readAllBytes(tmpFile.toPath());
        DataFrame d = DataFrame.load(data);
        Assert.assertEquals(file, d);

        String s = new String(data,"UTF-8");
        d = DataFrame.load(s);
        Assert.assertEquals(file, d);


        d = DataFrame.load(new FileInputStream(tmpFile));
        Assert.assertEquals(file, d);


        d = DataFrame.load(new FileReader(tmpFile));
        Assert.assertEquals(file, d);
        tmpFile.delete();


    }


    @Test
    public void autodetectTest() throws DataFrameException, IOException {
        CSVReader reader = CSVReaderBuilder.create()
                .withHeader(false)
                .withSeparator(';').build();
        DataFrame dataFrame = DataFrameLoader.load("loader_test.csv", DataFrameLoaderTest.class.getClassLoader(), reader);
        dataFrame.renameColumn("V1", "id");
        dataFrame.renameColumn("V2", "description");
        dataFrame.renameColumn("V3", "value");


        DataFrame dataFrameB = DataFrameLoader.loadResource("loader_test.csv", "loader_test.csv.meta", DataFrameLoaderTest.class.getClassLoader());

        Assert.assertEquals(dataFrame, dataFrameB);
    }

    @Test
    public void testMetaReader() throws Exception {
        DataFrame dataFrame = DataFrameLoader.loadResource("loader_test.csv", "loader_test.csv.meta", DataFrameLoaderTest.class.getClassLoader());
        Assert.assertEquals(5, dataFrame.size());

        /**
         1;1.2;A
         2;1.4;B
         3;1.6;C
         4;1.1;D
         5;1.2;E
         */
        String[] desc = new String[]{"A", "B", "C", "D", "E"};
        Double[] values = new Double[]{1.2, 1.4, 1.6, 1.1, 1.2};
        for (int i = 0; i < dataFrame.size(); i++) {
            DataRow row = dataFrame.getRow(i);
            Assert.assertEquals(i + 1, (int) row.getInteger("id"));
            Assert.assertEquals(desc[i], row.getString("description"));
            Assert.assertEquals(values[i], row.getDouble("value"));

        }


        Assert.assertEquals(IntegerColumn.class, dataFrame.getColumn("id").getClass());
        Assert.assertEquals(DoubleColumn.class, dataFrame.getColumn("value").getClass());
        Assert.assertEquals(StringColumn.class, dataFrame.getColumn("description").getClass());


        FilterPredicate predicate = FilterPredicate.and(
                FilterPredicate.lt("id", 5),
                FilterPredicate.ne("description", "B")
        );
        dataFrame = DataFrameLoader.loadResource("loader_test.csv", "loader_test.csv.meta",
                DataFrameLoaderTest.class.getClassLoader(), predicate);
        Assert.assertEquals(3, dataFrame.size());

        /**
         1;1.2;A
         3;1.6;C
         4;1.1;D
         */
        int[] ids = new int[]{1, 3, 4};
        desc = new String[]{"A", "C", "D"};
        values = new Double[]{1.2, 1.6, 1.1};
        for (int i = 0; i < dataFrame.size(); i++) {
            DataRow row = dataFrame.getRow(i);
            Assert.assertEquals(ids[i], (int) row.getInteger("id"));
            Assert.assertEquals(desc[i], row.getString("description"));
            Assert.assertEquals(values[i], row.getDouble("value"));

        }


        Assert.assertEquals(IntegerColumn.class, dataFrame.getColumn("id").getClass());
        Assert.assertEquals(DoubleColumn.class, dataFrame.getColumn("value").getClass());
        Assert.assertEquals(StringColumn.class, dataFrame.getColumn("description").getClass());
    }


}
