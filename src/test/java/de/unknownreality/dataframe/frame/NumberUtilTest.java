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

import de.unknownreality.dataframe.DataFrameException;
import de.unknownreality.dataframe.common.NumberUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Alex on 28.06.2017.
 */
public class NumberUtilTest {
    @Test
    public void testNumberUtilOperations() throws DataFrameException {
        Integer a = 1;
        Integer b = 2;
        Integer r = NumberUtil.add(a,b,Integer.class);
        Assert.assertEquals((Integer)3, r);

        Float rf = NumberUtil.add(a,b,Float.class);
        Assert.assertEquals((Float)3f, rf);


        a = 2;
        b = 3;
        r = NumberUtil.subtract(b,a,Integer.class);
        Assert.assertEquals((Integer)1, r);
        r = NumberUtil.subtract(a,b,Integer.class);
        Assert.assertEquals(new Integer(-1), r);
        rf = NumberUtil.subtract(a,b,Float.class);
        Assert.assertEquals(new Float(-1f), rf);


        r = NumberUtil.multiply(b,a,Integer.class);
        Assert.assertEquals((Integer)6, r);
        r = NumberUtil.multiply(a,b,Integer.class);
        Assert.assertEquals(new Integer(6), r);
        rf = NumberUtil.multiply(a,b,Float.class);
        Assert.assertEquals(new Float(6f), rf);

        a = 6;
        b = 3;
        r = NumberUtil.divide(b,a,Integer.class);
        Assert.assertEquals((Integer)0, r);
        r = NumberUtil.divide(a,b,Integer.class);
        Assert.assertEquals(new Integer(2), r);
        rf = NumberUtil.divide(a,b,Float.class);
        Assert.assertEquals(new Float(2f), rf);
        rf = NumberUtil.divide(b,a,Float.class);
        Assert.assertEquals(new Float(0.5f), rf);
    }

    @Test
    public void testNumberUtilConvert() throws DataFrameException {
        Assert.assertEquals(new Float(1f),
                NumberUtil.convert(new Integer(1),Float.class));

        Assert.assertEquals(new Double(1d),
                NumberUtil.convert(new Integer(1),Double.class));

        Assert.assertEquals(new Long(1l),
                NumberUtil.convert(new Integer(1),Long.class));

        Assert.assertEquals(new Short((short)1),
                NumberUtil.convert(new Integer(1),Short.class));

        Assert.assertEquals(new Byte((byte)1),
                NumberUtil.convert(new Integer(1),Byte.class));

        Assert.assertEquals(new Integer(1),
                NumberUtil.convert(new Double(1.5d),Integer.class));
    }
}
