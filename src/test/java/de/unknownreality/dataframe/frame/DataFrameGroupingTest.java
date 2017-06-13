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
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.column.BooleanColumn;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.group.DataGroup;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupRow;
import de.unknownreality.dataframe.group.aggr.Aggregate;
import de.unknownreality.dataframe.group.aggr.AggregateFunction;
import de.unknownreality.dataframe.transform.DataFrameTransform;
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
        DataFrame dataFrame = CSVReaderBuilder.create()
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
        DataGrouping dataGroups = dataFrame.groupBy("ID", "NAME").agg("MAX", Aggregate.max("VALUE",Integer.class));
        Assert.assertEquals(6, dataGroups.size());

        testGroup(dataGroups.findByGroupValues(1, "A"), 1, 6);
        testGroup(dataGroups.findByGroupValues(1, "B"), 2);
        testGroup(dataGroups.findByGroupValues(2, "A"), 3);
        testGroup(dataGroups.findByGroupValues(3, "B"), 4, 8);
        testGroup(dataGroups.findByGroupValues(2, "C"), 5);
        testGroup(dataGroups.findByGroupValues(4, "B"), 7);


        dataGroups.agg("count2",(group -> group.size()));
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
    public void testAgg(){
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
        dataFrame.append("d",4d,2,false,false,"123",1);
        dataFrame.append("a",3d,5,true,true,"abc123",1);
        dataFrame.append("b",2d,4,true,false,"abc/123",null);



        DataGrouping grouping = dataFrame
                .groupBy("name")
                .agg("count",Aggregate.count())
                .agg("mean", Aggregate.mean("x"))
                .agg("max",Aggregate.max("x"))
                .agg("min",group -> group.getDoubleColumn("x").min())
                .agg("na_count", group -> {
                    int c = 0;
                    for(int i  = 0; i < group.size(); i++){
                        if(group.getRow(i).isNA("n")){
                            c++;
                        }
                    }
                    return c;
                })
                .agg("desc",group -> group.getGroupDescription());


        System.out.println(grouping.getHeader());
        for(DataRow r : grouping){
            System.out.println(r);
        }

        DataFrame df = grouping.select("na_count < 3");
        System.out.println(df.getHeader());
        for(DataRow r : df){
            System.out.println(r);
        }
        df.getStringColumn("desc").map(value -> value+"::2");
        DataFrame joined = grouping.joinInner(df,"name");
        System.out.println(joined.getHeader());
        for(DataRow r : joined){
            System.out.println(r);
        }
    }


    public static void testGroup(GroupRow groupRow, int... values) {
        Assert.assertEquals(values.length, groupRow.getGroup().size());
        int i = 0;
        for (DataRow row : groupRow.getGroup()) {
            Assert.assertEquals(values[i++], (int) row.getInteger("VALUE"));
        }
    }


}
