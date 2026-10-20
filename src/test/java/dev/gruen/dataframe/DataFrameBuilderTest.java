/*
 *
 *  * Copyright (c) 2019-2026 Alexander Grün
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

package dev.gruen.dataframe;

import dev.gruen.dataframe.group.DataGrouping;
import dev.gruen.dataframe.group.GroupUtil;
import dev.gruen.dataframe.group.impl.TreeGroupUtil;
import dev.gruen.dataframe.join.JoinColumn;
import dev.gruen.dataframe.join.JoinUtil;
import dev.gruen.dataframe.join.JoinedDataFrame;
import dev.gruen.dataframe.join.impl.DefaultJoinUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

        dataFrame.append(true, (byte) 0, 1f, 1, 1L, (short) 1, "A");
        dataFrame.append(false, (byte) 1, 2f, 2, 2L, (short) 2, "B");

        Assertions.assertEquals(7, dataFrame.getColumns().size());
        Iterator<DataFrameColumn<?, ?>> columnIterator = dataFrame.getColumns().iterator();
        Assertions.assertEquals(true, columnIterator.next().get(0));
        Assertions.assertEquals((byte) 0, columnIterator.next().get(0));
        Assertions.assertEquals(1f, columnIterator.next().get(0));
        Assertions.assertEquals(1, columnIterator.next().get(0));
        Assertions.assertEquals(1L, columnIterator.next().get(0));
        Assertions.assertEquals((short) 1, columnIterator.next().get(0));
        Assertions.assertEquals("A", columnIterator.next().get(0));
        dataFrame.groupBy("boolean");
        Assertions.assertEquals(1, groupCount.get());

        DataFrame copy = dataFrame.copy();
        dataFrame.joinInner(copy, "byte");
        Assertions.assertEquals(1, joinCount.get());
    }
}
