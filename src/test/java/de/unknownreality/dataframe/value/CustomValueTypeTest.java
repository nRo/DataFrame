package de.unknownreality.dataframe.value;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.group.DataGrouping;
import org.junit.Assert;
import org.junit.Test;

public class CustomValueTypeTest {

    @Test
    public void customValueSortTest() {
        DataFrame df = DataFrame.create()
                .addColumn(new IntegerColumn("id"))
                .addColumn(new CustomColumn("x"));

        df.append(1, new CustomColumn.Custom(1, 1));
        df.append(2, new CustomColumn.Custom(2, 2));
        df.append(3, new CustomColumn.Custom(1, 2));
        df.append(4, new CustomColumn.Custom(2, 1));

        df = df.sort("x");

        Assert.assertEquals(1, df.getRow(0).getInteger("id").intValue());
        Assert.assertEquals(3, df.getRow(1).getInteger("id").intValue());
        Assert.assertEquals(4, df.getRow(2).getInteger("id").intValue());
        Assert.assertEquals(2, df.getRow(3).getInteger("id").intValue());
    }

    @Test
    public void customValueFilter() {
        DataFrame df = DataFrame.create()
                .addColumn(new IntegerColumn("id"))
                .addColumn(new CustomColumn("x"));

        df.append(1, new CustomColumn.Custom(1, 1));
        df.append(2, new CustomColumn.Custom(2, 2));
        df.append(3, new CustomColumn.Custom(1, 2));
        df.append(4, new CustomColumn.Custom(2, 1));

        df.filter("x == '[1,2]'");
        Assert.assertEquals(1, df.size());
        Assert.assertEquals(3, df.getRow(0).getInteger("id").intValue());
    }

    @Test
    public void customValueGroup() {
        DataFrame df = DataFrame.create()
                .addColumn(new IntegerColumn("id"))
                .addColumn(new CustomColumn("x"));

        df.append(1, new CustomColumn.Custom(1, 1));
        df.append(2, new CustomColumn.Custom(2, 2));
        df.append(3, new CustomColumn.Custom(1, 1));
        df.append(4, new CustomColumn.Custom(2, 2));
        DataGrouping g = df.groupBy("x");

        Assert.assertEquals(2, g.size());
        Assert.assertEquals(2, g.getGroup(0).size());
        Assert.assertEquals(2, g.getGroup(1).size());
        Assert.assertEquals(new CustomColumn.Custom(1, 1), g.getGroup(0).getGroupValues().get("x"));
        Assert.assertEquals(new CustomColumn.Custom(2, 2), g.getGroup(1).getGroupValues().get("x"));
    }


    @Test
    public void customValueJoin() {
        DataFrame dfA = DataFrame.create()
                .addColumn(new IntegerColumn("ida"))
                .addColumn(new CustomColumn("x"));

        dfA.append(1, new CustomColumn.Custom(1, 1));
        dfA.append(2, new CustomColumn.Custom(2, 1));
        dfA.append(3, new CustomColumn.Custom(1, 2));
        dfA.append(4, new CustomColumn.Custom(2, 2));

        DataFrame dfB = DataFrame.create()
                .addColumn(new IntegerColumn("idb"))
                .addColumn(new CustomColumn("x"));

        dfB.append(4, new CustomColumn.Custom(1, 1));
        dfB.append(3, new CustomColumn.Custom(2, 2));
        dfB.append(2, new CustomColumn.Custom(1, 1));
        dfB.append(1, new CustomColumn.Custom(2, 2));

        DataFrame joined = dfA.joinInner(dfB, "x");
        joined = joined.sort("ida");

        Assert.assertEquals(4, joined.size());
        Assert.assertEquals(1, joined.getRow(0).getInteger("ida").intValue());
        Assert.assertEquals(1, joined.getRow(1).getInteger("ida").intValue());
        Assert.assertEquals(4, joined.getRow(2).getInteger("ida").intValue());
        Assert.assertEquals(4, joined.getRow(3).getInteger("ida").intValue());

    }

}
