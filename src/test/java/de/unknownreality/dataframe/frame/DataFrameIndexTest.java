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
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.MapFunction;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameIndexTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testIndex() {
        /*
       ID;NAME;UID
        1;A;1
        1;B;2
        2;A;3
        3;A;4
        3;B;5
        3;C;6
        4;A;7
        3;A,8
         */
        DataFrame dataFrame = CSVReaderBuilder.create()
                .containsHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .loadResource("data_index.csv", DataFrameIndexTest.class.getClassLoader())
                .toDataFrame()
                .addColumn(new IntegerColumn("ID"))
                .addColumn(new StringColumn("NAME"))
                .addColumn(new IntegerColumn("UID")).build();
        Assert.assertEquals(8, dataFrame.size());

        dataFrame.setPrimaryKey("UID");

        Assert.assertEquals(1, (int) dataFrame.findByPrimaryKey(1).getInteger("ID"));
        Assert.assertEquals("A", dataFrame.findByPrimaryKey(1).getString("NAME"));

        Assert.assertEquals(1, (int) dataFrame.findByPrimaryKey(2).getInteger("ID"));
        Assert.assertEquals("B", dataFrame.findByPrimaryKey(2).getString("NAME"));

        Assert.assertEquals(2, (int) dataFrame.findByPrimaryKey(3).getInteger("ID"));
        Assert.assertEquals("A", dataFrame.findByPrimaryKey(3).getString("NAME"));

        dataFrame.addIndex("ID_NAME", "ID", "NAME");

        Assert.assertEquals(1, dataFrame.findByIndex("ID_NAME", 1, "A").size());
        Assert.assertEquals(1, dataFrame.findByIndex("ID_NAME", 1, "B").size());

        Assert.assertEquals(1, (int) dataFrame.findByIndex("ID_NAME", 1, "A").iterator().next().getInteger("UID"));
        Assert.assertEquals(2, (int) dataFrame.findByIndex("ID_NAME", 1, "B").iterator().next().getInteger("UID"));
        Assert.assertEquals(3, (int) dataFrame.findByIndex("ID_NAME", 2, "A").iterator().next().getInteger("UID"));

        List<DataRow> indexRows = dataFrame.findByIndex("ID_NAME",3,"A");
        Assert.assertEquals(2,indexRows.size());
        Set<Integer> expected = new HashSet<>();
        expected.add(4);
        expected.add(8);
        Set<Integer> values = new HashSet<>();
        for(DataRow row : indexRows){
            values.add(row.getInteger("UID"));
        }
        Assert.assertEquals(expected,values);

        dataFrame.append(1, "D", 9);
        dataFrame.append(2, "D", 10);
        dataFrame.append(5, "A", 11);
        dataFrame.append(3, "A", 12);


        Assert.assertEquals(1, (int) dataFrame.findByPrimaryKey(9).getInteger("ID"));
        Assert.assertEquals("D", dataFrame.findByPrimaryKey(9).getString("NAME"));

        Assert.assertEquals(5, (int) dataFrame.findByPrimaryKey(11).getInteger("ID"));
        Assert.assertEquals("A", dataFrame.findByPrimaryKey(11).getString("NAME"));

        dataFrame.getIntegerColumn("ID").map(new MapFunction<Integer>() {
            @Override
            public Integer map(Integer value) {
                return value + 2;
            }
        });

        Assert.assertEquals(3, (int) dataFrame.findByPrimaryKey(9).getInteger("ID"));


        Assert.assertEquals(1,  dataFrame.findByIndex("ID_NAME", 3, "B").size());
        Assert.assertEquals(2, (int) dataFrame.findByIndex("ID_NAME", 3, "B").iterator().next().getInteger("UID"));
        Assert.assertEquals(3, (int) dataFrame.findByIndex("ID_NAME", 4, "A").iterator().next().getInteger("UID"));

        Assert.assertEquals(3, (int) dataFrame.findByPrimaryKey(9).getInteger("ID"));
        Assert.assertEquals("D", dataFrame.findByPrimaryKey(9).getString("NAME"));

        Assert.assertEquals(7, (int) dataFrame.findByPrimaryKey(11).getInteger("ID"));
        Assert.assertEquals("A", dataFrame.findByPrimaryKey(11).getString("NAME"));

        DataRow row = dataFrame.findByIndex("ID_NAME", 3, "B").iterator().next();
        row.set("UID",999);
        dataFrame.update(row);
        Assert.assertEquals(999, (int) dataFrame.findByIndex("ID_NAME", 3, "B").iterator().next().getInteger("UID"));

        expected.add(12);

        indexRows = dataFrame.findByIndex("ID_NAME",5,"A");
        Assert.assertEquals(3,indexRows.size());
        values = new HashSet<>();
        for(DataRow r : indexRows){
            values.add(r.getInteger("UID"));
        }
        Assert.assertEquals(expected,values);
        dataFrame.removeIndex("ID_NAME");
        exception.expect(DataFrameRuntimeException.class);
        //UUID is primarykey -> must be unique
        dataFrame.append(3, "Z", 12);
        dataFrame.findByIndex("ID_NAME", 3, "B");
    }


}
