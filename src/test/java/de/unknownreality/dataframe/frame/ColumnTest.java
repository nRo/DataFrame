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

import de.unknownreality.dataframe.MapFunction;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.column.BasicColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Alex on 08.06.2017.
 */
public class ColumnTest {
    @Test
    public void testBasicColumn() {
        IntegerColumn basicColumn = new IntegerColumn("test");

        Assert.assertEquals(true, basicColumn.isEmpty());

        basicColumn.append(3);
        basicColumn.append(2);
        basicColumn.append(4);
        basicColumn.append(1);

        Integer[] a = new Integer[10];
        basicColumn.toArray(a);
        for(int i = 0; i < a.length;i++){
            if(i >= 4){
                Assert.assertEquals(null,a[i]);
            }
        }



        Assert.assertEquals(false, basicColumn.isEmpty());

        Assert.assertEquals(true, basicColumn.contains(1));
        Assert.assertEquals(false, basicColumn.contains(5));
        Assert.assertEquals(true,
                basicColumn.containsAll(Arrays.asList(new Integer[]{1,2}))
        );
        Assert.assertEquals(false,
                basicColumn.containsAll(Arrays.asList(new Integer[]{1,2,5}))
        );

        basicColumn.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return Double.compare(o1,o2);
            }
        });


        Integer[] values = basicColumn.toArray(new Integer[0]);
        Assert.assertEquals((Integer)1,values[0]);
        Assert.assertEquals((Integer)2,values[1]);
        Assert.assertEquals((Integer)3,values[2]);
        Assert.assertEquals((Integer)4,values[3]);


        Integer itCount = 1;
        Iterator<Integer> it = basicColumn.iterator();
        while(it.hasNext()){
            Integer v = it.next();
            Assert.assertEquals(itCount++,v);
        }


        basicColumn.reverse();
        values = basicColumn.toArray(new Integer[0]);
        Assert.assertEquals((Integer)1,values[3]);
        Assert.assertEquals((Integer)2,values[2]);
        Assert.assertEquals((Integer)3,values[1]);
        Assert.assertEquals((Integer)4,values[0]);

        basicColumn.map(new MapFunction<Integer>() {
            @Override
            public Integer map(Integer value) {
                return 10+value;
            }
        });
        values = basicColumn.toArray(new Integer[0]);
        Assert.assertEquals((Integer)11,values[3]);
        Assert.assertEquals((Integer)12,values[2]);
        Assert.assertEquals((Integer)13,values[1]);
        Assert.assertEquals((Integer)14,values[0]);

        int orgSize = basicColumn.size();
        basicColumn.append(14);
        Set<Integer> u = basicColumn.uniq();
        Assert.assertEquals(orgSize, u.size());

        basicColumn.appendNA();
        Assert.assertEquals(orgSize + 2, basicColumn.size());

        basicColumn.append(null);
        Assert.assertEquals(orgSize + 3, basicColumn.size());


       BasicColumn b = basicColumn;
       b.set(3,Values.NA);

       Assert.assertEquals(true, basicColumn.isNA(3));
        basicColumn.map(new MapFunction<Integer>() {
            @Override
            public Integer map(Integer value) {
                return value - 10;
            }
        });

        b.set(3,1);

        orgSize = basicColumn.size();
        for(int i = 0; i < BasicColumn.INIT_SIZE;i++){
            basicColumn.append(i);
        }
        Assert.assertEquals(orgSize + BasicColumn.INIT_SIZE, basicColumn.size());
        Assert.assertEquals((Integer)(BasicColumn.INIT_SIZE - 1), basicColumn.get(orgSize + BasicColumn.INIT_SIZE - 1));

        orgSize = basicColumn.size();
        basicColumn.appendAll(Arrays.asList(new Integer[]{1,2,3}));
        Assert.assertEquals(orgSize+3,basicColumn.size());
        Assert.assertEquals((Integer)3,basicColumn.get(basicColumn.size() - 1));

    }

    @Test(expected=UnsupportedOperationException.class)
    public void testErrors() {
        IntegerColumn basicColumn = new IntegerColumn("test");
        basicColumn.iterator().remove();
    }
}
