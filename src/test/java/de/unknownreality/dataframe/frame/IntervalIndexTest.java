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
import de.unknownreality.dataframe.filter.compile.PredicateCompiler;
import de.unknownreality.dataframe.filter.compile.PredicateCompilerException;
import de.unknownreality.dataframe.index.interval.IntervalIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IntervalIndexTest {

    @Test
    public void testIntervalIndex(){
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

        for(int i  = 0; i < starts.length; i++){
            dataFrame.append(names[i],starts[i],ends[i]);
        }

        IntervalIndex index = new IntervalIndex("idx",dataFrame.getNumberColumn("start"),dataFrame.getNumberColumn("end"));
        dataFrame.addIndex(index);
        Assert.assertEquals(2,index.getColumns().size());

        List<DataRow> rows;

        rows = dataFrame.findByIndex("idx",1d,3d);
        Assert.assertEquals(4,rows.size());


        rows = dataFrame.findByIndex("idx",9d,10d);
        Assert.assertEquals(1,rows.size());


        dataFrame = dataFrame.filter("name != 'G'");

        rows = dataFrame.findByIndex("idx",9d,10d);
        Assert.assertEquals(0,rows.size());

        rows = dataFrame.findByIndex("idx",8d,10d);
        Assert.assertEquals(1,rows.size());

        DataRow row =rows.get(0);
        row.set("start",11d);
        row.set("end",12);
        dataFrame.update(row);
        rows = dataFrame.findByIndex("idx",8d,10d);
        Assert.assertEquals(0,rows.size());

        rows = dataFrame.findByIndex("idx",10d,12d);
        Assert.assertEquals(1,rows.size());
    }
    @Test(expected=DataFrameRuntimeException.class)
    public void testMissingIndex() {
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addDoubleColumn("start")
                .addIntegerColumn("end");
       dataFrame.findByIndex("idx",9d,10d);


        Double[] starts = new Double[]{
                1d,
        };
        Integer[] ends = new Integer[]{
                2,
        };

        String[] names = new String[]{
                "A"
        };

        for(int i  = 0; i < starts.length; i++){
            dataFrame.append(names[i],starts[i],ends[i]);
        }


        IntervalIndex index = new IntervalIndex("idx",dataFrame.getNumberColumn("start"),dataFrame.getNumberColumn("end"));
        dataFrame.addIndex(index);

        List<DataRow> rows;

        rows = dataFrame.findByIndex("idx",1d,3d);
        Assert.assertEquals(1,rows.size());

        dataFrame.removeColumn("start");
        dataFrame.findByIndex("idx",1d,3d);
    }
}
