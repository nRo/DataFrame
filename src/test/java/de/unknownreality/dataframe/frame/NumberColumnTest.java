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

import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.FloatColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.common.math.Quantiles;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Alex on 17.07.2017.
 */
public class NumberColumnTest {

    @Test
    public void numbersTest() {
        DoubleColumn dc = new DoubleColumn("A");
        FloatColumn fc = new FloatColumn("B");
        IntegerColumn ic = new IntegerColumn("C");

        double sum = 0d;
        int count = 0;
        List<Double> dVals = new ArrayList<>();
        for (double d = 0d; d <= 10d; d++) {
            dc.append(d);
            sum += d;
            dVals.add(d);
            count++;
        }
        dc.appendNA();
        Assert.assertEquals(sum, dc.sum(), 0d);
        Assert.assertEquals(sum / count, dc.mean(), 0d);
        Assert.assertEquals(0d, dc.min(), 0d);
        Assert.assertEquals(10d, dc.max(), 0d);
        Assert.assertEquals(5d, dc.median(), 0d);

        List<Integer> iVals = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            ic.append(i);
            iVals.add(i);
        }
        ic.appendNA();
        DoubleColumn t = dc.copy();
        t.add(ic);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) + iVals.get(i), t.get(i), 0d);
        }
        t = dc.copy().subtract(ic);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) - iVals.get(i), t.get(i), 0d);
        }
        t = dc.copy().multiply(ic);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) * iVals.get(i), t.get(i), 0d);
        }
        t = dc.copy().divide(ic);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) / iVals.get(i), t.get(i), 0d);
        }

        t = dc.copy().add(5);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) + 5, t.get(i), 0d);
        }
        t = dc.copy().subtract(5);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) - 5, t.get(i), 0d);
        }
        t = dc.copy().multiply(5);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) * 5, t.get(i), 0d);
        }
        t = dc.copy().divide(5);
        for (int i = 0; i <= 10; i++) {
            Assert.assertEquals(dVals.get(i) / 5, t.get(i), 0d);
        }
    }


    @Test
    public void quantileTest(){
        DoubleColumn dc = new DoubleColumn("A");
        dc.appendAll(Arrays.asList(new Double[]{3d,2d,5d,1d,4d}));

        Quantiles<Double> quantiles = dc.getQuantiles();
        Assert.assertEquals((Double)1d,quantiles.min());
        Assert.assertEquals((Double)5d,quantiles.max());
        Assert.assertEquals((Double)3d,quantiles.median());
        Assert.assertEquals((Double)2d,quantiles.getQuantile(0.25));
        Assert.assertEquals((Double)4d,quantiles.getQuantile(0.75));

        Assert.assertEquals((Double)1d,dc.min());
        Assert.assertEquals((Double)5d,dc.max());
        Assert.assertEquals((Double)3d,dc.median());
        Assert.assertEquals((Double)2d,dc.getQuantile(0.25));
        Assert.assertEquals((Double)4d,dc.getQuantile(0.75));
    }
}
