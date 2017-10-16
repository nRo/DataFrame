package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameBuilder;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.group.DataGrouping;
import de.unknownreality.dataframe.group.GroupUtil;
import de.unknownreality.dataframe.group.impl.TreeGroupUtil;
import de.unknownreality.dataframe.join.JoinColumn;
import de.unknownreality.dataframe.join.JoinUtil;
import de.unknownreality.dataframe.join.JoinedDataFrame;
import de.unknownreality.dataframe.join.impl.DefaultJoinUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class DataFrameBuilderTest {
    @Test
    public void builderTest(){
        AtomicInteger groupCount = new AtomicInteger(0);

        GroupUtil groupUtil = new TreeGroupUtil(){
            @Override
            public DataGrouping groupBy(DataFrame df, String... columns) {
                groupCount.incrementAndGet();
                return super.groupBy(df, columns);
            }
        };
        AtomicInteger joinCount = new AtomicInteger(0);

        JoinUtil joinUtil = new DefaultJoinUtil(){
            @Override
            public JoinedDataFrame innerJoin(DataFrame dfA, DataFrame dfB, JoinColumn... joinColumns) {
                joinCount.incrementAndGet();
                return super.innerJoin(dfA, dfB, joinColumns);
            }
        };
        DataFrame dataFrame = DataFrameBuilder.create()
            .addBooleanColumn("boolean")
                .addByteColumn("byte")
                .addFloatColumn("float")
                .addIntegerColumn("integer")
                .addLongColumn("long")
                .addShortColumn("short")
                .addStringColumn("string")
                .setGroupUtil(groupUtil)
                .setJoinUtil(joinUtil).build();

        dataFrame.append(true,(byte)0,1f,1,1l,(short)1,"A");
        dataFrame.append(false,(byte)1,2f,2,2l,(short)2,"B");

        Assert.assertEquals(7,dataFrame.getColumns().size());
        Iterator<DataFrameColumn> columnIterator = dataFrame.getColumns().iterator();
        Assert.assertEquals(true,columnIterator.next().get(0));
        Assert.assertEquals((byte)0,columnIterator.next().get(0));
        Assert.assertEquals(1f,columnIterator.next().get(0));
        Assert.assertEquals(1,columnIterator.next().get(0));
        Assert.assertEquals(1l,columnIterator.next().get(0));
        Assert.assertEquals((short)1,columnIterator.next().get(0));
        Assert.assertEquals("A",columnIterator.next().get(0));
        dataFrame.groupBy("boolean");
        Assert.assertEquals(1,groupCount.get());

        DataFrame copy = dataFrame.copy();
        dataFrame.joinInner(copy,"byte");
        Assert.assertEquals(1,joinCount.get());
    }
}
