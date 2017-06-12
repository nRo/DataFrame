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

import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.DataGroup;
import de.unknownreality.dataframe.group.DataGrouping;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameGroupingTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testReader() {
        /*
        ID;NAME;VALUE
            1;A;1
            1;B;2
            2;A;3
            3;B;4
            2;C;5
            1;A;6
            4;B;7
            3;B;8
         */
        DefaultDataFrame dataFrame = CSVReaderBuilder.create()
                .containsHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .loadResource("data_grouping.csv", DataFrameGroupingTest.class.getClassLoader())
                .toDataFrame()
                .addColumn(new IntegerColumn("ID"))
                .addColumn(new StringColumn("NAME"))
                .addColumn(new IntegerColumn("VALUE")).build();
        Assert.assertEquals(8, dataFrame.size());

        /*
        Groups:
        1;A  (2)
        1;B  (1)
        2;A  (1)
        3;B  (2)
        2;C  (1)
        4;B  (1)
         */
        DataGrouping dataGroups = dataFrame.groupBy("ID", "NAME");
        Assert.assertEquals(6, dataGroups.size());

        testGroup(dataGroups.findByGroupValues(1, "A"), 1, 6);
        testGroup(dataGroups.findByGroupValues(1, "B"), 2);
        testGroup(dataGroups.findByGroupValues(2, "A"), 3);
        testGroup(dataGroups.findByGroupValues(3, "B"), 4, 8);
        testGroup(dataGroups.findByGroupValues(2, "C"), 5);
        testGroup(dataGroups.findByGroupValues(4, "B"), 7);

        DataGrouping grouping2 = dataGroups.find(FilterPredicate.and(FilterPredicate.lt("ID", 4), FilterPredicate.in("NAME", new String[]{"A", "B"})));
        Assert.assertEquals(4, grouping2.size());


    }


    public static void testGroup(DataGroup group, int... values) {
        Assert.assertEquals(values.length, group.size());
        int i = 0;
        for (DataRow row : group) {
            Assert.assertEquals(values[i++], (int) row.getInteger("VALUE"));
        }
    }


}
