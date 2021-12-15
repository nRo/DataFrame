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

package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.csv.CSVFormat;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.aggr.Aggregate;
import de.unknownreality.dataframe.group.impl.TreeGroupUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameGroupingTest {
    @Test
    public void testGroupUtil() {
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

        CSVReader csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .setColumnType("ID", Integer.class)
                .setColumnType("NAME", String.class)
                .setColumnType("VALUE", Integer.class)
                .build();

        DataFrame dataFrame = DataFrameLoader.load("data_grouping.csv", DataFrameGroupingTest.class.getClassLoader(), csvReader);
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
        DataGrouping dataGroups = dataFrame.groupBy("ID", "NAME")
                .agg("MAX", Aggregate.max("VALUE"))
                .agg("MIN", Aggregate.max("VALUE"));
        Assert.assertEquals(6, dataGroups.size());

        Assert.assertEquals("ID=1, NAME=A", dataGroups.findByGroupValues(1, "A").getGroup().getGroupDescription());
        Assert.assertEquals("ID=1, NAME=B", dataGroups.findByGroupValues(1, "B").getGroup().getGroupDescription());
        Assert.assertEquals("ID=2, NAME=A", dataGroups.findByGroupValues(2, "A").getGroup().getGroupDescription());
        Assert.assertEquals("ID=2, NAME=C", dataGroups.findByGroupValues(2, "C").getGroup().getGroupDescription());
        Assert.assertEquals("ID=3, NAME=B", dataGroups.findByGroupValues(3, "B").getGroup().getGroupDescription());
        Assert.assertEquals("ID=4, NAME=B", dataGroups.findByGroupValues(4, "B").getGroup().getGroupDescription());
        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MIN").getClass());

        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MIN").getClass());
        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MAX").getClass());


        testGroup(dataGroups.findByGroupValues(1, "A"), 1, 6);
        testGroup(dataGroups.findByGroupValues(1, "B"), 2);
        testGroup(dataGroups.findByGroupValues(2, "A"), 3);
        testGroup(dataGroups.findByGroupValues(3, "B"), 4, 8);
        testGroup(dataGroups.findByGroupValues(2, "C"), 5);
        testGroup(dataGroups.findByGroupValues(4, "B"), 7);


        dataGroups.agg("count2", (DataGroup::size));
        Assert.assertEquals((Integer) 2, dataGroups.findByGroupValues(1, "A").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(1, "B").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(2, "A").getInteger("count2"));
        Assert.assertEquals((Integer) 2, dataGroups.findByGroupValues(3, "B").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(2, "C").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(4, "B").getInteger("count2"));

        DataFrame grouping2 = dataGroups.select(FilterPredicate.and(FilterPredicate.lt("ID", 4), FilterPredicate.in("NAME", new String[]{"A", "B"})));
        Assert.assertEquals(4, grouping2.size());


    }

    @Test
    public void testNewGroupUtil() {
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

        CSVReader csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .setColumnType("ID", Integer.class)
                .setColumnType("NAME", String.class)
                .setColumnType("VALUE", Integer.class)
                .build();

        DataFrame dataFrame = DataFrameLoader.load("data_grouping.csv", DataFrameGroupingTest.class.getClassLoader(), csvReader);
        ((DefaultDataFrame) dataFrame).setGroupUtil(new TreeGroupUtil());
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
        DataGrouping dataGroups = dataFrame.groupBy("ID", "NAME")
                .agg("MAX", Aggregate.max("VALUE"))
                .agg("MIN", Aggregate.max("VALUE"));
        Assert.assertEquals(6, dataGroups.size());

        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MIN").getClass());
        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MAX").getClass());


        testGroup(dataGroups.findByGroupValues(1, "A"), 1, 6);
        testGroup(dataGroups.findByGroupValues(1, "B"), 2);
        testGroup(dataGroups.findByGroupValues(2, "A"), 3);
        testGroup(dataGroups.findByGroupValues(3, "B"), 4, 8);
        testGroup(dataGroups.findByGroupValues(2, "C"), 5);
        testGroup(dataGroups.findByGroupValues(4, "B"), 7);


        dataGroups.agg("count2", (DataGroup::size));
        Assert.assertEquals((Integer) 2, dataGroups.findByGroupValues(1, "A").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(1, "B").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(2, "A").getInteger("count2"));
        Assert.assertEquals((Integer) 2, dataGroups.findByGroupValues(3, "B").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(2, "C").getInteger("count2"));
        Assert.assertEquals((Integer) 1, dataGroups.findByGroupValues(4, "B").getInteger("count2"));

        DataFrame grouping2 = dataGroups.select(FilterPredicate.and(FilterPredicate.lt("ID", 4), FilterPredicate.in("NAME", new String[]{"A", "B"})));
        Assert.assertEquals(4, grouping2.size());


    }

    @Test
    public void testAgg() {
        DataFrame dataFrame = DataFrame.fromCSV("data_group_agg_pre.csv", DataFrame.class.getClassLoader(), ';', true);
        dataFrame.print(new CSVFormat());
        DataGrouping grouping = dataFrame
                .groupBy("name")
                .agg("count", Aggregate.count())
                .agg("mean", Aggregate.mean("x"))
                .agg("max", Aggregate.max("x"))
                .agg("na_count", Aggregate.naCount("n"))
                .agg("filter_count", Aggregate.filterCount("r ~= /[a-z].+/"))
                .agg("first", Aggregate.first("x"))
                .agg("nfirst", Aggregate.first("n"))
                .agg("x_25", Aggregate.quantile("x", 0.25))
                .agg("desc", DataGroup::getGroupDescription);

        DataFrame result = DataFrame.fromCSV("data_group_agg_result.csv", DataFrame.class.getClassLoader(), ';', true);
        Assert.assertEquals(result, grouping);
    }

    @Test
    public void testMaxAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));

        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(2, 4.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("max_value", Aggregate.max("value"));
        Assert.assertEquals(3.0, grouping.getValue(1, 0));
        Assert.assertEquals(4.0, grouping.getValue(1, 1));
        Assert.assertEquals(2.0, grouping.getValue(1, 2));
    }

    @Test
    public void testNumberFailAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new StringColumn("name"));

        dataFrame.append(1, "x");
        dataFrame.append(1, "y");
        dataFrame.append(1, "z");
        dataFrame.append(2, "x");
        dataFrame.append(2, "y");
        dataFrame.append(3, "z");
        dataFrame.append(3, null);
        Assert.assertThrows("column 'name' has wrong type", DataFrameRuntimeException.class,
                () -> dataFrame.groupBy("id")
                        .agg("names", Aggregate.mean("name")));
        Assert.assertThrows("column 'name' has wrong type", DataFrameRuntimeException.class,
                () -> dataFrame.groupBy("id")
                        .agg("names", Aggregate.min("name")));
        Assert.assertThrows("column 'name' has wrong type", DataFrameRuntimeException.class,
                () -> dataFrame.groupBy("id")
                        .agg("names", Aggregate.max("name")));
        Assert.assertThrows("column 'name' has wrong type", DataFrameRuntimeException.class,
                () -> dataFrame.groupBy("id")
                        .agg("names", Aggregate.median("name")));
    }

    @Test
    public void testMinAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));

        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(2, 4.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("min_value", Aggregate.min("value"));
        Assert.assertEquals(1.0, grouping.getValue(1, 0));
        Assert.assertEquals(1.0, grouping.getValue(1, 1));
        Assert.assertEquals(2.0, grouping.getValue(1, 2));
    }

    @Test
    public void testFirstAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));
        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(2, 4.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("first_value", Aggregate.first("value"));
        Assert.assertEquals(1.0, grouping.getValue(1, 0));
        Assert.assertEquals(4.0, grouping.getValue(1, 1));
        Assert.assertEquals(2.0, grouping.getValue(1, 2));
    }

    @Test
    public void testLastAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));
        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(2, 4.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("first_value", Aggregate.last("value"));
        Assert.assertEquals(3.0, grouping.getValue(1, 0));
        Assert.assertEquals(1.0, grouping.getValue(1, 1));
        Assert.assertNull(grouping.getValue(1, 2));
    }

    @Test
    public void testNACountAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));

        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(2, null);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, Values.NA);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("avg_value", Aggregate.naCount("value"));
        Assert.assertEquals(0, grouping.getValue(1, 0));
        Assert.assertEquals(1, grouping.getValue(1, 1));
        Assert.assertEquals(2, grouping.getValue(1, 2));
    }

    @Test
    public void testMeanAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));

        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(2, 4.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("mean_value", Aggregate.mean("value"));
        Assert.assertEquals(2.0, grouping.getValue(1, 0));
        Assert.assertEquals(2.5, grouping.getValue(1, 1));
        Assert.assertEquals(2.0, grouping.getValue(1, 2));
    }

    @Test
    public void testMedianAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));

        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 3.0);
        dataFrame.append(1, 8.0);
        dataFrame.append(1, 50.0);
        dataFrame.append(2, 4.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("median_value", Aggregate.median("value"));
        Assert.assertEquals(3.0, grouping.getValue(1, 0));
        Assert.assertEquals(4.0, grouping.getValue(1, 1));
        Assert.assertEquals(2.0, grouping.getValue(1, 2));
    }

    @Test
    public void testQuantile25Agg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new DoubleColumn("value"));

        dataFrame.append(1, 0.5);
        dataFrame.append(1, 1.0);
        dataFrame.append(1, 2.0);
        dataFrame.append(1, 4.0);
        dataFrame.append(1, 5.0);
        dataFrame.append(1, 6.0);
        dataFrame.append(1, 7.0);
        dataFrame.append(1, 8.0);
        dataFrame.append(2, 8.0);
        dataFrame.append(2, 1.0);
        dataFrame.append(3, 2.0);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("mean_value", Aggregate.quantile("value", 0.25));
        Assert.assertEquals(1.0, grouping.getValue(1, 0));
        Assert.assertEquals(1.0, grouping.getValue(1, 1));
        Assert.assertEquals(2.0, grouping.getValue(1, 2));
    }

    @Test
    public void testJoinAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new StringColumn("name"));

        dataFrame.append(1, "x");
        dataFrame.append(1, "y");
        dataFrame.append(1, "z");
        dataFrame.append(2, "x");
        dataFrame.append(2, "y");
        dataFrame.append(3, "z");
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("names", Aggregate.join(",", "name"));
        Assert.assertEquals("x,y,z", grouping.getValue(1, 0));
        Assert.assertEquals("x,y", grouping.getValue(1, 1));
        Assert.assertEquals("z,", grouping.getValue(1, 2));
    }

    @Test
    public void testFilterAgg() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumn(new StringColumn("name"));

        dataFrame.append(1, "xbc");
        dataFrame.append(1, "ybc");
        dataFrame.append(1, "zbc1");
        dataFrame.append(2, "x");
        dataFrame.append(2, "y");
        dataFrame.append(3, "z3");
        dataFrame.append(3, null);
        DataGrouping grouping = dataFrame.groupBy("id")
                .agg("names", Aggregate.filterCount("name ~= /[a-z]+[0-9]/"));
        Assert.assertEquals(1, grouping.getValue(1, 0));
        Assert.assertEquals(0, grouping.getValue(1, 1));
        Assert.assertEquals(1, grouping.getValue(1, 2));
    }

    public static void testGroup(GroupRow groupRow, int... values) {
        Assert.assertEquals(values.length, groupRow.getGroup().size());
        int i = 0;
        for (DataRow row : groupRow.getGroup()) {
            Assert.assertEquals(values[i++], (int) row.getInteger("VALUE"));
        }
    }


}
