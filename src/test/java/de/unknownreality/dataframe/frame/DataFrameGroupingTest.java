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

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.column.BooleanColumn;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.DataGroup;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupRow;
import de.unknownreality.dataframe.group.aggr.Aggregate;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.group.impl.TreeGroupUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameGroupingTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testGroupUtil() throws IOException {
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
                .setColumnType("ID",Integer.class)
                .setColumnType("NAME",String.class)
                .setColumnType("VALUE",Integer.class)
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
                .agg("MIN",Aggregate.max("VALUE"));
        Assert.assertEquals(6, dataGroups.size());

        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MIN").getClass());
        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MAX").getClass());


        testGroup(dataGroups.findByGroupValues(1, "A"), 1, 6);
        testGroup(dataGroups.findByGroupValues(1, "B"), 2);
        testGroup(dataGroups.findByGroupValues(2, "A"), 3);
        testGroup(dataGroups.findByGroupValues(3, "B"), 4, 8);
        testGroup(dataGroups.findByGroupValues(2, "C"), 5);
        testGroup(dataGroups.findByGroupValues(4, "B"), 7);


        dataGroups.agg("count2",(DataGroup::size));
        Assert.assertEquals((Integer)2,dataGroups.findByGroupValues(1, "A").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(1, "B").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(2, "A").getInteger("count2"));
        Assert.assertEquals((Integer)2,dataGroups.findByGroupValues(3, "B").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(2, "C").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(4, "B").getInteger("count2"));

        DataFrame grouping2 = dataGroups.select(FilterPredicate.and(FilterPredicate.lt("ID", 4), FilterPredicate.in("NAME", new String[]{"A", "B"})));
        Assert.assertEquals(4, grouping2.size());


    }

    @Test
    public void testNewGroupUtil() throws IOException {
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
                .setColumnType("ID",Integer.class)
                .setColumnType("NAME",String.class)
                .setColumnType("VALUE",Integer.class)
                .build();

        DataFrame dataFrame = DataFrameLoader.load("data_grouping.csv", DataFrameGroupingTest.class.getClassLoader(), csvReader);
        ((DefaultDataFrame)dataFrame).setGroupUtil(new TreeGroupUtil());
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
                .agg("MIN",Aggregate.max("VALUE"));
        Assert.assertEquals(6, dataGroups.size());

        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MIN").getClass());
        Assert.assertEquals(IntegerColumn.class, dataGroups.getColumn("MAX").getClass());


        testGroup(dataGroups.findByGroupValues(1, "A"), 1, 6);
        testGroup(dataGroups.findByGroupValues(1, "B"), 2);
        testGroup(dataGroups.findByGroupValues(2, "A"), 3);
        testGroup(dataGroups.findByGroupValues(3, "B"), 4, 8);
        testGroup(dataGroups.findByGroupValues(2, "C"), 5);
        testGroup(dataGroups.findByGroupValues(4, "B"), 7);


        dataGroups.agg("count2",(DataGroup::size));
        Assert.assertEquals((Integer)2,dataGroups.findByGroupValues(1, "A").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(1, "B").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(2, "A").getInteger("count2"));
        Assert.assertEquals((Integer)2,dataGroups.findByGroupValues(3, "B").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(2, "C").getInteger("count2"));
        Assert.assertEquals((Integer)1,dataGroups.findByGroupValues(4, "B").getInteger("count2"));

        DataFrame grouping2 = dataGroups.select(FilterPredicate.and(FilterPredicate.lt("ID", 4), FilterPredicate.in("NAME", new String[]{"A", "B"})));
        Assert.assertEquals(4, grouping2.size());


    }

    @Test
    public void testAgg() throws IOException {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new BooleanColumn("z"));
        dataFrame.addColumn(new BooleanColumn("v"));
        dataFrame.addColumn(new StringColumn("r"));
        dataFrame.addColumn(new IntegerColumn("n"));


        dataFrame.append("a",1d,5,true,true,"abc123",1);
        dataFrame.append("b",2d,4,true,false,"abc/123",null);
        dataFrame.append("c",3d,3,false,true,"abc", Values.NA);
        dataFrame.append("d",4d,2,false,false,"123",1);
        dataFrame.append("a",2d,5,true,true,"abc123",1);
        dataFrame.append("b",2d,4,true,false,"abc/123",null);
        dataFrame.append("c",3d,3,false,true,"abc", Values.NA);
        dataFrame.append("d",4d,2,false,false,"a123",1);
        dataFrame.append("a",3d,5,true,true,"1bc123",1);
        dataFrame.append("b",2d,4,true,false,"abc/123",null);



        DataGrouping grouping = dataFrame
                .groupBy("name")
                .agg("count",Aggregate.count())
                .agg("mean", Aggregate.mean("x"))
                .agg("max",Aggregate.max("x"))
                .agg("na_count", Aggregate.naCount("n"))
                .agg("filter_count",Aggregate.filterCount("r ~= /[a-z].+/"))
                .agg("first", Aggregate.first("x"))
                .agg("x_25", Aggregate.quantile("x",0.25))
                .agg("desc",group -> group.getGroupDescription());

        for(DataRow row : grouping){
            DataGroup group = grouping.getGroup(row.getIndex());
            System.out.println(group.getGroupDescription());
        }

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
        writer.write("test");
        writer.newLine();

        grouping.print();

        DataFrame df = grouping.select("na_count < 3");
        df.print();

        df.getStringColumn("desc").map(value -> value+"::2");
        DataFrame joined = grouping.joinInner(df,"name");
        joined.print();

    }


    public static void testGroup(GroupRow groupRow, int... values) {
        Assert.assertEquals(values.length, groupRow.getGroup().size());
        int i = 0;
        for (DataRow row : groupRow.getGroup()) {
            Assert.assertEquals(values[i++], (int) row.getInteger("VALUE"));
        }
    }


}
