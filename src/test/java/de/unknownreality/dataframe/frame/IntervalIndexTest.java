/*
 *
 *  * Copyright (c) 2017 Alexander Grün
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

package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DataRows;
import de.unknownreality.dataframe.column.LongColumn;
import de.unknownreality.dataframe.index.interval.IntervalIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IntervalIndexTest {

    @Test
    public void testIntervalIndex() {
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addDoubleColumn("start")
                .addIntegerColumn("end");

        Double[] starts = new Double[]{
                1d,
                1.5d,
                2d,
                3d,
                3.2d,
                3.8d,
                5d,
        };
        Integer[] ends = new Integer[]{
                2,
                3,
                5,
                6,
                7,
                8,
                9
        };

        String[] names = new String[]{
                "A",
                "B",
                "C",
                "D",
                "E",
                "F",
                "G"
        };

        for (int i = 0; i < starts.length; i++) {
            dataFrame.append(names[i], starts[i], ends[i]);
        }

        IntervalIndex index = new IntervalIndex("idx", dataFrame.getNumberColumn("start"), dataFrame.getNumberColumn("end"));
        dataFrame.addIndex(index);
        Assert.assertEquals(2, index.getColumns().size());

        List<DataRow> rows;

        rows = dataFrame.selectRowsByIndex("idx", 1d, 3d);
        Assert.assertEquals(4, rows.size());


        rows = dataFrame.selectRowsByIndex("idx", 9d, 10d);
        Assert.assertEquals(1, rows.size());


        dataFrame = dataFrame.filter("name != 'G'");

        rows = dataFrame.selectRowsByIndex("idx", 9d, 10d);
        Assert.assertEquals(0, rows.size());

        rows = dataFrame.selectRowsByIndex("idx", 8d, 10d);
        Assert.assertEquals(1, rows.size());

        DataRow row = rows.get(0);
        row.set("start", 11);
        row.set("end", 12);
        //dataFrame.update(row);
        rows = dataFrame.selectRowsByIndex("idx", 8d, 10d);
        Assert.assertEquals(0, rows.size());

        rows = dataFrame.selectRowsByIndex("idx", 10d, 12d);
        Assert.assertEquals(1, rows.size());

        rows = dataFrame.selectRowsByIndex("idx", 3);
        Assert.assertEquals(3, rows.size());

        try {
            dataFrame.selectRowsByIndex("idx");
            fail("Expected a DataFrameRuntimeException to be thrown");
        } catch (DataFrameRuntimeException dataFrameRuntimeException) {
            assertEquals(dataFrameRuntimeException.getMessage(),
                    "start and end values are required for interval search");
        }

        try {
            dataFrame.selectRowsByIndex("idx", "x", 12d);
            fail("Expected a DataFrameRuntimeException to be thrown");
        } catch (DataFrameRuntimeException dataFrameRuntimeException) {
            assertEquals(dataFrameRuntimeException.getMessage(),
                    "start and end values must be numbers for interval search");
        }

        try {
            dataFrame.selectRowsByIndex("idx", "x");
            fail("Expected a DataFrameRuntimeException to be thrown");
        } catch (DataFrameRuntimeException dataFrameRuntimeException) {
            assertEquals(dataFrameRuntimeException.getMessage(),
                    "stab value must be a number for interval search");
        }

    }

    @Test
    public void testConstructorsAndUniqueAndReplace() {
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addLongColumn("start")
                .addLongColumn("end");

        dataFrame.append("A", 1L, 2L);
        dataFrame.append("B", 3L, 4L);


        IntervalIndex intervalIndex = IntervalIndex.create(dataFrame, "interval", "start", "end");
        Assert.assertEquals(dataFrame.getLongColumn("start"), intervalIndex.getColumns().get(0));
        Assert.assertEquals(dataFrame.getLongColumn("end"), intervalIndex.getColumns().get(1));


        intervalIndex = IntervalIndex.create("interval",
                dataFrame.getLongColumn("start"),
                dataFrame.getLongColumn("end"));
        Assert.assertEquals(dataFrame.getLongColumn("start"), intervalIndex.getColumns().get(0));
        Assert.assertEquals(dataFrame.getLongColumn("end"), intervalIndex.getColumns().get(1));

        Assert.assertEquals(false, intervalIndex.isUnique());
        Assert.assertEquals(true, intervalIndex.containsColumn(dataFrame.getLongColumn("start")));
        Assert.assertEquals(true, intervalIndex.containsColumn(dataFrame.getLongColumn("end")));

        try {
            intervalIndex.setUnique(true);
            fail("Expected a DataFrameRuntimeException to be thrown");
        } catch (DataFrameRuntimeException dataFrameRuntimeException) {
            assertEquals(dataFrameRuntimeException.getMessage(),
                    "unique is not supported by interval indices");
        }

        dataFrame.addIndex(intervalIndex);
        DataRows rows = dataFrame.selectRowsByIndex("interval", 4, 5);
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("B", rows.get(0).getString("name"));

        rows = dataFrame.selectRowsByIndex("interval", 5, 6);
        Assert.assertEquals(0, rows.size());

        LongColumn replaceColumn = dataFrame.getLongColumn("end").copy();
        replaceColumn.map((value -> value + 1));

        dataFrame.replaceColumn("end", replaceColumn);

        rows = dataFrame.selectRowsByIndex("interval", 5, 6);
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("B", rows.get(0).getString("name"));

        intervalIndex.remove(dataFrame.getRow(1));
        rows = dataFrame.selectRowsByIndex("interval", 5, 6);
        Assert.assertEquals(0, rows.size());

    }

    @Test(expected = DataFrameRuntimeException.class)
    public void testMissingIndex() {
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addDoubleColumn("start")
                .addIntegerColumn("end");
        dataFrame.selectRowsByIndex("idx", 9d, 10d);

        dataFrame.append("A", 1, 2);


        dataFrame.addIndex(IntervalIndex.create(dataFrame, "idx", "start", "end"));

        List<DataRow> rows;

        rows = dataFrame.selectRowsByIndex("idx", 1d, 3d);
        Assert.assertEquals(1, rows.size());

        dataFrame.removeColumn("start");
        dataFrame.selectRowsByIndex("idx", 1d, 3d);
    }
}
