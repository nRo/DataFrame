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

import de.unknownreality.dataframe.*;
import de.unknownreality.dataframe.column.*;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.filter.FilterPredicate;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();


    @Test
    public void testCreation() {
        DataFrame dataFrame = DataFrameBuilder.createDefault();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumns(new StringColumn("first"), new StringColumn("last"));
        dataFrame.addColumn(Double.class, "value", ColumnTypeMap.create().addType(Double.class, DoubleColumn.class));
        dataFrame.removeColumn("value");
        dataFrame.addColumn(Double.class, "value");
        dataFrame.append(1, "A", "B", 1d);
        Assert.assertEquals(1, dataFrame.size());
        dataFrame.addColumn(StringColumn.class, "full", new ColumnAppender<String>() {
            @Override
            public String createRowValue(DataRow row) {
                return row.getString("first") + "-" + row.getString("last");
            }
        });
        Assert.assertEquals("A-B", dataFrame.getRow(0).getString("full"));
        dataFrame.addColumn(Integer.class, "int_value", ColumnTypeMap.create(), new ColumnAppender<Integer>() {
            @Override
            public Integer createRowValue(DataRow row) {
                return row.getNumber("value").intValue();
            }
        });
        Assert.assertEquals(1, (int) dataFrame.getRow(0).getInteger("int_value"));
        dataFrame.removeColumn(dataFrame.getIntegerColumn("int_value"));
        dataFrame.addColumn(Integer.class, "int_value");
        int colIndex = dataFrame.getHeader().getIndex("int_value");
        Assert.assertEquals(5, colIndex);
        Assert.assertEquals(true, dataFrame.getRow(0).isNA(colIndex));
        dataFrame.removeColumn("int_value");

        int oldSize = dataFrame.size();

        dataFrame.removeColumn("___");
        Assert.assertEquals(oldSize, dataFrame.size());

        DataFrame dataFrame2 = new DefaultDataFrame();
        dataFrame2.addColumn(new IntegerColumn("id"));
        dataFrame2.addColumn(new StringColumn("first"));
        dataFrame2.addColumn(new StringColumn("last"));
        dataFrame2.addColumn(new StringColumn("full"));

        DoubleColumn valueColumn = new DoubleColumn("value");
        dataFrame2.addColumn(valueColumn);
        dataFrame2.append(2, "A", "C", "A-C", 2d);
        dataFrame = dataFrame.concat(dataFrame2);
        Assert.assertEquals(2, dataFrame.size());
        Assert.assertEquals(2d, dataFrame.getRow(1).toDouble("value"), 0d);


        Assert.assertEquals(valueColumn, dataFrame2.getDoubleColumn("value"));
        Assert.assertEquals(valueColumn, dataFrame2.getNumberColumn("value"));
        Assert.assertEquals(valueColumn, dataFrame2.getColumn("value"));
    }

    @Test
    public void testReader() {
        String[] header = new String[]{"A", "B", "C", "D", "E","F","G","H","I"};
        Integer[] col1 = new Integer[]{5, 3, 2, 4, 1};
        Double[] col2 = new Double[]{1d, 3d, 5d, 5d, 2d};
        String[] col3 = new String[]{"", "Y", "Y", "X", ""};
        Boolean[] col4 = new Boolean[]{false, false, true, false, true};
        String[] col5 = new String[]{"0", "1", "2", "3", "4", "5"};
        Byte[] col6 = new Byte[]{0, 0, 1, 1, 0, 1};
        Short[] col7 = new Short[]{0, 0, 1, 1, 0, 1};
        Long[] col8 = new Long[]{0l, 0l, 1l, 1l, 0l, 1l};
        Float[] col9 = new Float[]{0f, 0f, 1f, 1f, 0f, 1f};

        String csv = createCSV(header, col1, col2, col3, col4, col5, col6, col7, col8, col9);

        CSVReader reader = CSVReaderBuilder.create()
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .containsHeader(true).load(csv);

        DataFrame df = reader.toDataFrame()
                .addColumn(new IntegerColumn("A"))
                .addColumn(new DoubleColumn("B"))
                .addColumn(new StringColumn("C"))
                .addColumn(new BooleanColumn("D"))
                .addColumn(new StringColumn("E"))
                .addColumn(new ByteColumn("F"))
                .addColumn(new ShortColumn("G"))
                .addColumn(new LongColumn("H"))
                .addColumn(new FloatColumn("I"))
                .build();

        reader = CSVReaderBuilder.create()
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .containsHeader(true).load(csv);

        DataFrame df2 = reader.toDataFrame()
                .addIntegerColumn("A")
                .addDoubleColumn("B")
                .addStringColumn("C")
                .addBooleanColumn("D")
                .addStringColumn("E")
                .addByteColumn("F")
                .addShortColumn("G")
                .addLongColumn("H")
                .addFloatColumn("I")
                .build();


        Assert.assertEquals(df, df2);

        df.iterator();


        Assert.assertEquals(header.length, df.getHeader().size());
        for (int i = 0; i < header.length; i++) {
            Assert.assertEquals(header[i], df.getHeader().get(i));
        }
        Assert.assertEquals(col1.length, df.size());
        int i = 0;
        for (DataRow row : df) {
            Assert.assertEquals(col1[i], row.get(0));
            Assert.assertEquals(col2[i], row.get(1));
            Assert.assertEquals(col3[i].equals("") ? Values.NA : col3[i], row.get(2));
            Assert.assertEquals(col4[i], row.get(3));

            Assert.assertEquals(col1[i], row.get(header[0]));
            Assert.assertEquals(col2[i], row.get(header[1]));
            Assert.assertEquals(col3[i].equals("") ? Values.NA : col3[i], row.get(header[2]));
            Assert.assertEquals(col4[i], row.get(header[3]));

            Assert.assertEquals(i, row.toDouble("E").intValue());
            row.getInteger(0);
            row.getDouble(1);
            row.getString(2);
            row.getBoolean(3);
            i++;
        }

        DataRow row = df.getRow(1);
        row.set("A", 999);
        df.update(row);

        Assert.assertEquals(new Integer(999), df.getRow(1).getInteger("A"));

        df.getColumn("A").sort();
    }

    @Test
    public void rowAccessTest(){
        String[] header = new String[]{"A", "B", "C", "D", "E","F","G","H","I"};
        Integer[] col1 = new Integer[]{5, 3, 2, 4, 1};
        Double[] col2 = new Double[]{1d, 3d, 5d, 5d, 2d};
        String[] col3 = new String[]{"", "Y", "Y", "X", ""};
        Boolean[] col4 = new Boolean[]{false, false, true, false, true};
        String[] col5 = new String[]{"0", "1", "2", "3", "4", "5"};
        Byte[] col6 = new Byte[]{0, 0, 1, 1, 0, 1};
        Short[] col7 = new Short[]{0, 0, 1, 1, 0, 1};
        Long[] col8 = new Long[]{0l, 0l, 1l, 1l, 0l, 1l};
        Float[] col9 = new Float[]{0f, 0f, 1f, 1f, 0f, 1f};

        String csv = createCSV(header, col1, col2, col3, col4, col5, col6, col7, col8, col9);

        CSVReader reader = CSVReaderBuilder.create()
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .containsHeader(true).load(csv);

        DataFrame df = reader.toDataFrame()
                .addColumn(new IntegerColumn("A"))
                .addColumn(new DoubleColumn("B"))
                .addColumn(new StringColumn("C"))
                .addColumn(new BooleanColumn("D"))
                .addColumn(new StringColumn("E"))
                .addColumn(new ByteColumn("F"))
                .addColumn(new ShortColumn("G"))
                .addColumn(new LongColumn("H"))
                .addColumn(new FloatColumn("I"))
                .build();

        Assert.assertEquals(1l, df.getRow(2).get("H"));
        Assert.assertEquals(1l, df.getRow(2).getLong("H").longValue());
        Assert.assertEquals(1l, df.getRow(2).getLong(7).longValue());
        Assert.assertEquals(1l, df.getRow(2).getLong("H").longValue());
        Assert.assertEquals(1l, df.getRow(2).getLong(7).longValue());

        Assert.assertEquals(df.getRow(2).getNumber("I").doubleValue(), df.getRow(2).getNumber("H").doubleValue(),0d);
        Assert.assertEquals(new Short((short)1), df.getRow(2).getShort("G"));
        Assert.assertEquals(new Short((short)1), df.getRow(2).getShort(6));

        Assert.assertEquals(new Byte((byte) 1), df.getRow(2).getByte("F"));
        Assert.assertEquals(new Byte((byte) 1), df.getRow(2).getByte(5));

        Assert.assertEquals(1f, df.getRow(2).getFloat("I"),0f);
        Assert.assertEquals(1f, df.getRow(2).getFloat(8),0f);

        Assert.assertEquals((Integer)2, df.getRow(2).getInteger("A"));
        Assert.assertEquals((Integer)2, df.getRow(2).getInteger(0));

        Assert.assertEquals((Double)5d, df.getRow(2).getDouble("B"));
        Assert.assertEquals((Double)5d, df.getRow(2).getDouble(1));
        Assert.assertEquals("Y", df.getRow(2).getString("C"));
        Assert.assertEquals("Y", df.getRow(2).getString(2));
    }

    @Test
    public void predicateTest() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new BooleanColumn("z"));
        dataFrame.addColumn(new BooleanColumn("v"));
        dataFrame.addColumn(new StringColumn("r"));


        dataFrame.append("a", 1d, 5, true, true, "abc123");
        dataFrame.append("b", 2d, 4, true, false, "abc/123");
        dataFrame.append("c", 3d, 3, false, true, "abc");
        dataFrame.append("d", 4d, 2, false, false, "123");

        DataFrame filtered = dataFrame.select(
                FilterPredicate.and(
                        FilterPredicate.ne("name", "a").and(FilterPredicate.ne("name", "b")),
                        FilterPredicate.lt("x", 4).or(FilterPredicate.gt("x", 3))
                ));
        Assert.assertEquals(2, filtered.size());

        filtered = dataFrame.select(
                FilterPredicate.eq("z", true).nor(FilterPredicate.eq("v", true))
        );
        Assert.assertEquals(1, filtered.size());

        filtered = dataFrame.select(
                FilterPredicate.eq("z", true).xor(FilterPredicate.eq("v", true))
        );
        Assert.assertEquals(2, filtered.size());

        filtered = dataFrame.select(
                FilterPredicate.in("name", new String[]{"a", "b"}).neg()
        );
        Assert.assertEquals(2, filtered.size());
        Assert.assertEquals("c", filtered.getRow(0).getString("name"));
        Assert.assertEquals("d", filtered.getRow(1).getString("name"));

        filtered = dataFrame.select(
                FilterPredicate.eq("name", "a").neg()
        );
        Assert.assertEquals(3, filtered.size());
        Assert.assertEquals("b", filtered.getRow(0).getString("name"));
        Assert.assertEquals("c", filtered.getRow(1).getString("name"));
        Assert.assertEquals("d", filtered.getRow(2).getString("name"));

    }

    @Test
    public void testEquals() {
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new BooleanColumn("z"));
        dataFrame.addColumn(new BooleanColumn("v"));
        dataFrame.addColumn(new StringColumn("r"));


        dataFrame.append("a", 1d, 5, true, true, "abc123");
        dataFrame.append("b", 2d, 4, true, false, "abc/123");
        dataFrame.append("c", 3d, 3, false, true, "abc");
        dataFrame.append("d", 4d, 2, false, false, "123");

        DataFrame dataFrame2 = new DefaultDataFrame();
        dataFrame2.addColumn(new StringColumn("name"));
        dataFrame2.addColumn(new DoubleColumn("x"));
        dataFrame2.addColumn(new IntegerColumn("y"));
        dataFrame2.addColumn(new BooleanColumn("z"));
        dataFrame2.addColumn(new BooleanColumn("v"));
        dataFrame2.addColumn(new StringColumn("r"));


        dataFrame2.append("a", 1d, 5, true, true, "abc123");
        dataFrame2.append("b", 2d, 4, true, false, "abc/123");
        dataFrame2.append("c", 3d, 3, false, true, "abc");
        dataFrame2.append("d", 4d, 2, false, false, "123");

        Assert.assertEquals(dataFrame, dataFrame2);

        DataRow r = dataFrame2.getRow(0);
        r.set("name", "abc");
        dataFrame2.update(r);
        Assert.assertNotEquals(dataFrame, dataFrame2);

        DataFrame dataFrame3 = dataFrame.copy();
        Assert.assertEquals(dataFrame, dataFrame3);


    }

    @Test
    public void testNA() {

        Assert.assertEquals(true, Values.NA.isNA(Values.NA));
        Assert.assertEquals(true, Values.NA.isNA("NA"));
        Assert.assertEquals(false, Values.NA.isNA(null));
        Assert.assertEquals(false, Values.NA.isNA(1));
        Assert.assertEquals(false, Values.NA.isNA("na"));
        Assert.assertEquals(0, Values.NA.compareTo(Values.NA));
        Assert.assertEquals(-1, Values.NA.compareTo(1));
        Assert.assertEquals(-1, Values.NA.compareTo(-1));


        String[] header = new String[]{"A", "B", "C", "D"};
        Integer[] col1 = new Integer[]{5, 3, 2, 4, 1};
        Double[] col2 = new Double[]{2d, 3d, null, 5d, 2d};
        String[] col3 = new String[]{"X", "Y", "X", Values.NA.toString(), "X"};
        Boolean[] col4 = new Boolean[]{false, false, true, false, true};
        String csv = createCSV(header, col1, col2, col3, col4);

        CSVReader reader = CSVReaderBuilder.create()
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .containsHeader(true).load(csv);

        DataFrame df = reader.toDataFrame()
                .addColumn(new IntegerColumn("A"))
                .addColumn(new DoubleColumn("B"))
                .addColumn(new StringColumn("C"))
                .addColumn(new BooleanColumn("D"))
                .build();
        List<DataRow> r = df.getRows();
        int i = 0;
        for (DataRow row : df.rows()) {
            if (i == 2) {
                Assert.assertEquals(true, row.isNA("B"));
            } else {
                Assert.assertEquals(false, row.isNA("B"));
            }
            if (i == 3) {
                // Assert.assertEquals(true, row.isNA("C"));
                Assert.assertEquals(Values.NA, row.get("C"));
            } else {
                Assert.assertEquals(false, row.isNA("C"));
            }
            i++;
        }

        DoubleColumn dc = df.getDoubleColumn("B");
        Assert.assertEquals(col2.length, dc.size());
        Assert.assertEquals(12d, dc.sum(), 0d);
        Assert.assertEquals(3d, dc.mean(), 0d);
        Assert.assertEquals(5d, dc.max(), 0d);
        Assert.assertEquals(2d, dc.min(), 0d);

    }


    @Test
    public void numbersSortedTest(){
        final AtomicInteger sortCount = new AtomicInteger(0);
        DoubleColumn testColumn = new DoubleColumn("test"){
            @Override
            protected Double[] getSortedValues() {
                if(requireSorting()){
                    sortCount.incrementAndGet();
                }
                return super.getSortedValues();
            }
        };

        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(testColumn);
        dataFrame.append(2d);
        dataFrame.append(3d);
        dataFrame.append(1d);
        dataFrame.append(6d);
        dataFrame.append(5d);
        dataFrame.append(4d);

        Assert.assertEquals(0,sortCount.get());
        Assert.assertEquals((Double)1d, testColumn.getQuantile(0d));
        Assert.assertEquals(1,sortCount.get());
        Assert.assertEquals((Double)6d,testColumn.getQuantile(1d));
        Assert.assertEquals(1,sortCount.get());
        dataFrame.append(7d);
        dataFrame.append(8d);
        Assert.assertEquals((Double)8d,testColumn.getQuantile(1d));
        Assert.assertEquals(2,sortCount.get());
        Assert.assertEquals((Double)4d,testColumn.getQuantile(0.5));
        Assert.assertEquals(2,sortCount.get());
        Assert.assertEquals((Double)2d,testColumn.getQuantile(0.25));
        Assert.assertEquals(2,sortCount.get());

        testColumn.map((value -> value*2d));
        Assert.assertEquals((Double)8d,testColumn.getQuantile(0.5));
        Assert.assertEquals(3,sortCount.get());

    }

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

    private String createCSV(String[] head, Object[]... cols) {
        StringBuilder sb = new StringBuilder();
        sb.append("#");
        for (int i = 0; i < head.length; i++) {
            sb.append(head[i]);
            if (i < head.length - 1) {
                sb.append("\t");
            }
        }
        sb.append("\n");
        for (int i = 0; i < cols[0].length; i++) {
            for (int j = 0; j < head.length; j++) {
                sb.append(cols[j][i]);
                if (j < head.length - 1) {
                    sb.append("\t");
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
