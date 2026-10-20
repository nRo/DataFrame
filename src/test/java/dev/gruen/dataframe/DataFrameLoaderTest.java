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
import dev.gruen.dataframe.csv.CSVReader;
import dev.gruen.dataframe.csv.CSVReaderBuilder;
import dev.gruen.dataframe.filter.FilterPredicate;
import dev.gruen.dataframe.group.DataFrameGroupingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    @Test
    public void builderFromIteratorTest() throws IOException {
        CSVReader reader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .build();
        DataFrame frame = DataFrame.builder()
                .from(reader.load("data_grouping.csv", DataFrameGroupingTest.class.getClassLoader()))
                .addColumn(new IntegerColumn("ID"))
                .addColumn(new StringColumn("NAME"))
                .addColumn(new IntegerColumn("VALUE")).build();
        Assertions.assertEquals(3, frame.getColumns().size());
        Assertions.assertTrue(frame.size() > 0);
    }
    @Test
    public void loaderTest() throws IOException {

        DataFrame res = DataFrame.fromCSV("loader_test.csv", DataFrameLoaderTest.class.getClassLoader(), ';', false);
        Assertions.assertEquals(5, res.size());
        Assertions.assertEquals(3, res.getColumns().size());
        CSVReader csvReader = CSVReaderBuilder.create().withSeparator(';').withHeader(false).build();
        DataFrame test = DataFrame.load("loader_test.csv",DataFrameLoaderTest.class.getClassLoader(),csvReader);
        Assertions.assertEquals(res,test);

        File tmpFile = File.createTempFile("dataframe", ".csv");
        res.writeCSV(tmpFile,';', false);

        URI uri = tmpFile.toURI();
        DataFrame url = DataFrame.fromCSV(uri.toURL(),';', false);





        Assertions.assertEquals(res, url);
        res.write(tmpFile);
        DataFrame file = DataFrame.load(tmpFile);
        Assertions.assertEquals(file, url);

        byte[] data = Files.readAllBytes(tmpFile.toPath());
        DataFrame d = DataFrame.load(data);
        Assertions.assertEquals(file, d);

        String s = new String(data,"UTF-8");
        d = DataFrame.load(s);
        Assertions.assertEquals(file, d);


        d = DataFrame.load(new FileInputStream(tmpFile));
        Assertions.assertEquals(file, d);


        d = DataFrame.load(new FileReader(tmpFile));
        Assertions.assertEquals(file, d);
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

        Assertions.assertEquals(dataFrame, dataFrameB);
    }

    @Test
    public void testMetaReader() throws Exception {
        DataFrame dataFrame = DataFrameLoader.loadResource("loader_test.csv", "loader_test.csv.meta", DataFrameLoaderTest.class.getClassLoader());
        Assertions.assertEquals(5, dataFrame.size());

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
            Assertions.assertEquals(i + 1, (int) row.getInteger("id"));
            Assertions.assertEquals(desc[i], row.getString("description"));
            Assertions.assertEquals(values[i], row.getDouble("value"));

        }


        Assertions.assertEquals(IntegerColumn.class, dataFrame.getColumn("id").getClass());
        Assertions.assertEquals(DoubleColumn.class, dataFrame.getColumn("value").getClass());
        Assertions.assertEquals(StringColumn.class, dataFrame.getColumn("description").getClass());


        FilterPredicate predicate = FilterPredicate.and(
                FilterPredicate.lt("id", 5),
                FilterPredicate.ne("description", "B")
        );
        dataFrame = DataFrameLoader.loadResource("loader_test.csv", "loader_test.csv.meta",
                DataFrameLoaderTest.class.getClassLoader(), predicate);
        Assertions.assertEquals(3, dataFrame.size());

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
            Assertions.assertEquals(ids[i], (int) row.getInteger("id"));
            Assertions.assertEquals(desc[i], row.getString("description"));
            Assertions.assertEquals(values[i], row.getDouble("value"));

        }


        Assertions.assertEquals(IntegerColumn.class, dataFrame.getColumn("id").getClass());
        Assertions.assertEquals(DoubleColumn.class, dataFrame.getColumn("value").getClass());
        Assertions.assertEquals(StringColumn.class, dataFrame.getColumn("description").getClass());
    }


}
