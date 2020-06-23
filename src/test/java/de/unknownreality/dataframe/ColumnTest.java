/*
 *
 *  * Copyright (c) 2019 Alexander Grün
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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.column.*;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

/**
 * Created by Alex on 08.06.2017.
 */
public class ColumnTest {
    @Test
    public void testBasicColumn() {
        IntegerColumn basicColumn = new IntegerColumn("test");

        Assert.assertTrue(basicColumn.isEmpty());

        basicColumn.append(3);
        basicColumn.append(2);
        basicColumn.append(4);
        basicColumn.append(1);

        Integer[] a = new Integer[10];
        basicColumn.toArray(a);
        for (int i = 0; i < a.length; i++) {
            if (i >= 4) {
                Assert.assertNull(a[i]);
            }
        }


        Assert.assertFalse(basicColumn.isEmpty());

        Assert.assertTrue(basicColumn.contains(1));
        Assert.assertFalse(basicColumn.contains(5));
        Assert.assertTrue(basicColumn.containsAll(Arrays.asList(1, 2)));
        Assert.assertFalse(basicColumn.containsAll(Arrays.asList(1, 2, 5)));

        basicColumn.sort(Comparator.comparingDouble(o -> o));


        Integer[] values = basicColumn.toArray(new Integer[0]);
        Assert.assertEquals((Integer) 1, values[0]);
        Assert.assertEquals((Integer) 2, values[1]);
        Assert.assertEquals((Integer) 3, values[2]);
        Assert.assertEquals((Integer) 4, values[3]);


        Integer itCount = 1;
        for (Integer v : basicColumn) {
            Assert.assertEquals(itCount++, v);
        }


        basicColumn.reverse();
        values = basicColumn.toArray(new Integer[0]);
        Assert.assertEquals((Integer) 1, values[3]);
        Assert.assertEquals((Integer) 2, values[2]);
        Assert.assertEquals((Integer) 3, values[1]);
        Assert.assertEquals((Integer) 4, values[0]);

        basicColumn.map(value -> 10 + value);
        values = basicColumn.toArray(new Integer[0]);
        Assert.assertEquals((Integer) 11, values[3]);
        Assert.assertEquals((Integer) 12, values[2]);
        Assert.assertEquals((Integer) 13, values[1]);
        Assert.assertEquals((Integer) 14, values[0]);

        int orgSize = basicColumn.size();
        basicColumn.append(14);
        Set<Integer> u = basicColumn.uniq();
        Assert.assertEquals(orgSize, u.size());

        basicColumn.appendNA();
        Assert.assertEquals(orgSize + 2, basicColumn.size());

        basicColumn.append(null);
        Assert.assertEquals(orgSize + 3, basicColumn.size());


        BasicColumn b = basicColumn;
        b.set(3, Values.NA);

        Assert.assertEquals(true, basicColumn.isNA(3));
        basicColumn.map(value -> value - 10);

        b.set(3, 1);

        orgSize = basicColumn.size();
        for (int i = 0; i < BasicColumn.INIT_SIZE; i++) {
            basicColumn.append(i);
        }
        Assert.assertEquals(orgSize + BasicColumn.INIT_SIZE, basicColumn.size());
        Assert.assertEquals((Integer) (BasicColumn.INIT_SIZE - 1), basicColumn.get(orgSize + BasicColumn.INIT_SIZE - 1));

        orgSize = basicColumn.size();
        basicColumn.appendAll(Arrays.asList(new Integer[]{1, 2, 3}));
        Assert.assertEquals(orgSize + 3, basicColumn.size());
        Assert.assertEquals((Integer) 3, basicColumn.get(basicColumn.size() - 1));

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testErrors() {
        IntegerColumn basicColumn = new IntegerColumn("test");
        basicColumn.iterator().remove();
    }

    @Test
    public void testBooleanColumn() {
        BooleanColumn column = new BooleanColumn();
        column.append(true);
        column.append(false);
        column.appendAll(Arrays.asList(new Boolean[]{true, true, false}));
        Assert.assertEquals(5, column.size());
        BooleanColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, false);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new BooleanColumn("test", column.toArray(new Boolean[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, false);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new BooleanColumn("test", column.toArray(new Boolean[0]), 2);
        Assert.assertEquals(2, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));
        Assert.assertEquals(column.get(1), copyColumn.get(1));


        BooleanColumn a = new BooleanColumn("a", new Boolean[]{true, true, false, false});

        BooleanColumn b = new BooleanColumn("b", new Boolean[]{true, false, false, true});

        Assert.assertArrayEquals(
                new Boolean[]{true, false, false, false}
                , a.copy().and(b)
                        .toArray());

        Assert.assertArrayEquals(
                new Boolean[]{false, true, false, false}
                , a.copy().andNot(b)
                        .toArray());

        Assert.assertArrayEquals(
                new Boolean[]{true, true, false, true}
                , a.copy().or(b)
                        .toArray());

        Assert.assertArrayEquals(
                new Boolean[]{false, true, false, true}
                , a.copy().xor(b)
                        .toArray());

        Assert.assertArrayEquals(
                new Boolean[]{false, false, true, true}
                , a.copy().flip()
                        .toArray());

        try {
            Assert.assertEquals(
                    (Boolean) true, column.getValueType().parse("true")
            );
            Assert.assertEquals(
                    (Boolean) false, column.getValueType().parse("false")
            );
            Assert.assertEquals(
                    (Boolean) true, column.getValueType().parse("TRUE")
            );
            Assert.assertEquals(
                    (Boolean) false, column.getValueType().parse("FALSE")
            );
            Assert.assertEquals(
                    null, column.getValueType().parseOrNull("false1")
            );
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testByteColumn() {
        ByteColumn column = new ByteColumn();
        column.append((byte) 1);
        column.append((byte) 0);
        Assert.assertEquals(2, column.size());
        ByteColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, (byte) 0);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new ByteColumn("test", column.toArray(new Byte[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, (byte) 0);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new ByteColumn("test", column.toArray(new Byte[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        try {
            Assert.assertEquals(
                    new Byte((byte) 1), column.getValueType().parse("1")
            );
            Assert.assertEquals(
                    new Byte((byte) 2), column.getValueType().parse("2")
            );
            Assert.assertEquals(
                    null, column.getValueType().parseOrNull("x")
            );
            Assert.assertEquals(
                    null, column.getValueType().parseOrNull("10000000")
            );
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDoubleColumn() {
        DoubleColumn column = new DoubleColumn();
        column.append(1d);
        column.append(0d);
        Assert.assertEquals(2, column.size());
        DoubleColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, 0d);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new DoubleColumn("test", column.toArray(new Double[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, 0d);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new DoubleColumn("test", column.toArray(new Double[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        Assert.assertEquals(
                (Double) 1d, column.getValueType().parse("1")
        );
        Assert.assertEquals(
                (Double) 1.5d, column.getValueType().parse("1.5")
        );
        Assert.assertEquals(
                (Double) (-1.5d), column.getValueType().parse("-1.5")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("abc")
        );
    }

    @Test
    public void testFloatColumn() {
        FloatColumn column = new FloatColumn();
        column.append(1f);
        column.append(0f);
        Assert.assertEquals(2, column.size());
        FloatColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, 0f);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new FloatColumn("test", column.toArray(new Float[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, 0f);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new FloatColumn("test", column.toArray(new Float[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        Assert.assertEquals(
                (Float) 1f, column.getValueType().parse("1")
        );
        Assert.assertEquals(
                (Float) 1.5f, column.getValueType().parse("1.5")
        );
        Assert.assertEquals(
                (Float) (-1.5f), column.getValueType().parse("-1.5")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("abc")
        );
    }

    @Test
    public void testIntegerColumn() {
        IntegerColumn column = new IntegerColumn();
        column.append(1);
        column.append(0);
        Assert.assertEquals(2, column.size());
        IntegerColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, 0);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new IntegerColumn("test", column.toArray(new Integer[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, 0);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new IntegerColumn("test", column.toArray(new Integer[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        Assert.assertEquals(
                (Integer) 1, column.getValueType().parse("1")
        );
        Assert.assertEquals(
                (Integer) (-1), column.getValueType().parse("-1")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("1.5")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("abc")
        );
    }

    @Test
    public void testLongColumn() {
        LongColumn column = new LongColumn();
        column.append(1L);
        column.append(0L);
        Assert.assertEquals(2, column.size());
        LongColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, 0L);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new LongColumn("test", column.toArray(new Long[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, 0L);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new LongColumn("test", column.toArray(new Long[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        Assert.assertEquals(
                (Long) 1L, column.getValueType().parse("1")
        );
        Assert.assertEquals(
                (Long) (-1L), column.getValueType().parse("-1")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("1.5")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("abc")
        );
    }

    @Test
    public void testShortColumn() {
        ShortColumn column = new ShortColumn();
        column.append((short) 1);
        column.append((short) 0);
        Assert.assertEquals(2, column.size());
        ShortColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, (short) 0);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new ShortColumn("test", column.toArray(new Short[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, (short) 0);
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new ShortColumn("test", column.toArray(new Short[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        Assert.assertEquals(
                (Short) (short) 1, column.getValueType().parse("1")
        );
        Assert.assertEquals(
                (Short) (short) (-1), column.getValueType().parse("-1")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("1.5")
        );
        Assert.assertEquals(
                null, column.getValueType().parseOrNull("abc")
        );
    }

    @Test
    public void testStringColumn() {
        StringColumn column = new StringColumn();
        column.append("1");
        column.append("2");
        Assert.assertEquals(2, column.size());
        StringColumn copyColumn = column.copyEmpty();
        Assert.assertEquals(0, copyColumn.size());

        copyColumn = column.copy();
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());

        copyColumn.set(0, "2");
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));


        copyColumn = new StringColumn("test", column.toArray(new String[0]));
        Assert.assertArrayEquals(column.toArray(), copyColumn.toArray());
        copyColumn.set(0, "2");
        Assert.assertNotEquals(column.get(0), copyColumn.get(0));

        copyColumn = new StringColumn("test", column.toArray(new String[0]), 1);
        Assert.assertEquals(1, copyColumn.size());
        Assert.assertEquals(column.get(0), copyColumn.get(0));


        Assert.assertEquals(
                "1", column.getValueType().parse("1")
        );
    }

    @Test
    public void rowValueTest() {
        DataFrame dataFrame = DataFrame.create();
        dataFrame.addBooleanColumn("boolean");
        dataFrame.addByteColumn("byte");
        dataFrame.addDoubleColumn("double");
        dataFrame.addFloatColumn("float");
        dataFrame.addIntegerColumn("integer");
        dataFrame.addLongColumn("long");
        dataFrame.addShortColumn("short");
        dataFrame.addStringColumn("string");

        Object[] values = new Object[]{true, (byte) 1, 2.5d, 3.5f, 4, 5L, (short) 6, "7"};

        dataFrame.append(values);
        DataRow row = dataFrame.getRow(0);

        Assert.assertEquals(values[0],
                dataFrame.getBooleanColumn("boolean").getValueFromRow(row, "boolean"));

        Assert.assertEquals(values[0],
                dataFrame.getBooleanColumn("boolean").getValueFromRow(row, 0));

        Assert.assertEquals(values[1],
                dataFrame.getByteColumn("byte").getValueFromRow(row, "byte"));

        Assert.assertEquals(values[1],
                dataFrame.getByteColumn("byte").getValueFromRow(row, 1));

        Assert.assertEquals(values[2],
                dataFrame.getDoubleColumn("double").getValueFromRow(row, "double"));

        Assert.assertEquals(values[2],
                dataFrame.getDoubleColumn("double").getValueFromRow(row, 2));

        Assert.assertEquals(values[3],
                dataFrame.getFloatColumn("float").getValueFromRow(row, "float"));

        Assert.assertEquals(values[3],
                dataFrame.getFloatColumn("float").getValueFromRow(row, 3));

        Assert.assertEquals(values[4],
                dataFrame.getIntegerColumn("integer").getValueFromRow(row, "integer"));

        Assert.assertEquals(values[4],
                dataFrame.getIntegerColumn("integer").getValueFromRow(row, 4));

        Assert.assertEquals(values[5],
                dataFrame.getLongColumn("long").getValueFromRow(row, "long"));

        Assert.assertEquals(values[5],
                dataFrame.getLongColumn("long").getValueFromRow(row, 5));

        Assert.assertEquals(values[6],
                dataFrame.getShortColumn("short").getValueFromRow(row, "short"));

        Assert.assertEquals(values[6],
                dataFrame.getShortColumn("short").getValueFromRow(row, 6));

        Assert.assertEquals(values[7],
                dataFrame.getStringColumn("string").getValueFromRow(row, "string"));

        Assert.assertEquals(values[7],
                dataFrame.getStringColumn("string").getValueFromRow(row, 7));

    }
}
