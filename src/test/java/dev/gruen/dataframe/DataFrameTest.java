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

import dev.gruen.dataframe.column.BooleanColumn;
import dev.gruen.dataframe.column.DoubleColumn;
import dev.gruen.dataframe.column.IntegerColumn;
import dev.gruen.dataframe.column.StringColumn;
import dev.gruen.dataframe.csv.CSVReader;
import dev.gruen.dataframe.csv.CSVReaderBuilder;
import dev.gruen.dataframe.filter.FilterPredicate;
import dev.gruen.dataframe.sort.SortColumn;
import dev.gruen.dataframe.type.DataFrameTypeManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameTest {

    @Test
    public void testCreation() {
        DataFrame dataFrame = DataFrameBuilder.createDefault();
        dataFrame.addColumn(new IntegerColumn("id"));
        dataFrame.addColumns(new StringColumn("first"), new StringColumn("last"));
        dataFrame.addColumn(Double.class, "value", DataFrameTypeManager.createNew().register(new DoubleColumn()));
        dataFrame.removeColumn("value");
        dataFrame.addColumn(Double.class, "value");
        dataFrame.append(1, "A", "B", 1d);
        Assertions.assertEquals(1, dataFrame.size());
        dataFrame.addColumn(StringColumn.class, "full", row -> row.getString("first") + "-" + row.getString("last"));
        Assertions.assertEquals("A-B", dataFrame.getRow(0).getString("full"));
        dataFrame.addColumn(Integer.class, "int_value", DataFrameTypeManager.createNew(), row -> row.getNumber("value").intValue());
        Assertions.assertEquals(1, (int) dataFrame.getRow(0).getInteger("int_value"));
        dataFrame.removeColumn(dataFrame.getIntegerColumn("int_value"));
        dataFrame.addColumn(Integer.class, "int_value");
        int colIndex = dataFrame.getHeader().getIndex("int_value");
        Assertions.assertEquals(5, colIndex);
        Assertions.assertEquals(true, dataFrame.getRow(0).isNA(colIndex));
        dataFrame.removeColumn("int_value");

        int oldSize = dataFrame.size();

        dataFrame.removeColumn("___");
        Assertions.assertEquals(oldSize, dataFrame.size());

        DataFrame dataFrame2 = new DefaultDataFrame();
        dataFrame2.addColumn(new IntegerColumn("id"));
        dataFrame2.addColumn(new StringColumn("first"));
        dataFrame2.addColumn(new StringColumn("last"));
        dataFrame2.addColumn(new StringColumn("full"));

        DoubleColumn valueColumn = new DoubleColumn("value");
        dataFrame2.addColumn(valueColumn);
        dataFrame2.append(2, "A", "C", "A-C", 2d);
        try{
            dataFrame2.append("x","A","C","A-B",2d);
            fail("error expected");
        }
        catch (Exception e){

        }
        dataFrame = dataFrame.concat(dataFrame2);
        dataFrame.addStringColumn("null_test");
        Assertions.assertEquals(2, dataFrame.size());
        Assertions.assertEquals(2d, dataFrame.getRow(1).toDouble("value"), 0d);
        Assertions.assertEquals("NA", dataFrame.getRow(1).getString("null_test"));
        Assertions.assertEquals(true, dataFrame.getRow(1).isNA("null_test"));


        Assertions.assertEquals(valueColumn, dataFrame2.getDoubleColumn("value"));
        Assertions.assertEquals(valueColumn, dataFrame2.getNumberColumn("value"));
        Assertions.assertEquals(valueColumn, dataFrame2.getColumn("value"));
    }

    @Test
    public void testReader() throws IOException {
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

        CSVReader csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .setColumnType("A",Integer.class)
                .setColumnType("B",Double.class)
                .setColumnType("C",String.class)
                .setColumnType("D",Boolean.class)
                .setColumnType("E",String.class)
                .setColumnType("F",Byte.class)
                .setColumnType("G",Short.class)
                .setColumnType("H",Long.class)
                .setColumnType("I",Float.class)
                .build();


        DataFrame df = DataFrameLoader.load(csv,csvReader);






        DataFrame df2 = DataFrameLoader.load(csv,csvReader);



        Assertions.assertEquals(df, df2);

        df.iterator();


        Assertions.assertEquals(header.length, df.getHeader().size());
        for (int i = 0; i < header.length; i++) {
            Assertions.assertEquals(header[i], df.getHeader().get(i));
        }
        Assertions.assertEquals(col1.length, df.size());
        int i = 0;
        for (DataRow row : df) {
            Assertions.assertEquals(col1[i], row.get(0));
            Assertions.assertEquals(col2[i], row.get(1));
            Assertions.assertEquals(col3[i].equals("") ? Values.NA : col3[i], row.get(2));
            Assertions.assertEquals(col4[i], row.get(3));

            Assertions.assertEquals(col1[i], row.get(header[0]));
            Assertions.assertEquals(col2[i], row.get(header[1]));
            Assertions.assertEquals(col3[i].equals("") ? Values.NA : col3[i], row.get(header[2]));
            Assertions.assertEquals(col4[i], row.get(header[3]));

            Assertions.assertEquals(i, row.toDouble("E").intValue());
            row.getInteger(0);
            row.getDouble(1);
            row.getString(2);
            row.getBoolean(3);
            i++;
        }

        DataRow row = df.getRow(1);
        row.set("A", 999);

        Assertions.assertEquals(Integer.valueOf(999), df.getRow(1).getInteger("A"));
        row.set("A", null);
        Assertions.assertEquals(true, df.getRow(1).isNA("A"));

        row.set("A", Values.NA);
        Assertions.assertEquals(true, df.getRow(1).isNA("A"));
        df.getColumn("A").sort();
    }

    @Test
    public  void clearTest(){
        DataFrame df = DataFrame.create()
                .addIntegerColumn("x");
        df.append(1).append(2);
        Assertions.assertEquals(2,df.size());
        Assertions.assertEquals(1,df.getColumns().size());
        Assertions.assertEquals(2,df.getIntegerColumn("x").size());
        df.clear();
        Assertions.assertEquals(0,df.size());
        Assertions.assertEquals(1,df.getColumns().size());
        Assertions.assertEquals(0,df.getIntegerColumn("x").size());
    }

    @Test
    public void rowAccessTest() throws IOException {
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

        CSVReader csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .setColumnType("A",Integer.class)
                .setColumnType("B",Double.class)
                .setColumnType("C",String.class)
                .setColumnType("D",Boolean.class)
                .setColumnType("E",String.class)
                .setColumnType("F",Byte.class)
                .setColumnType("G",Short.class)
                .setColumnType("H",Long.class)
                .setColumnType("I",Float.class)
                .build();


        DataFrame df = DataFrameLoader.load(csv,csvReader);




        Assertions.assertEquals(1l, df.getRow(2).get("H"));
        Assertions.assertEquals(1l, df.getRow(2).getLong("H").longValue());
        Assertions.assertEquals(1l, df.getRow(2).getLong(7).longValue());
        Assertions.assertEquals(1l, df.getRow(2).getLong("H").longValue());
        Assertions.assertEquals(1l, df.getRow(2).getLong(7).longValue());

        Assertions.assertEquals(df.getRow(2).getNumber("I").doubleValue(), df.getRow(2).getNumber("H").doubleValue(),0d);
        Assertions.assertEquals(new Short((short)1), df.getRow(2).getShort("G"));
        Assertions.assertEquals(new Short((short)1), df.getRow(2).getShort(6));

        Assertions.assertEquals(new Byte((byte) 1), df.getRow(2).getByte("F"));
        Assertions.assertEquals(new Byte((byte) 1), df.getRow(2).getByte(5));

        Assertions.assertEquals(1f, df.getRow(2).getFloat("I"),0f);
        Assertions.assertEquals(1f, df.getRow(2).getFloat(8),0f);

        Assertions.assertEquals((Integer)2, df.getRow(2).getInteger("A"));
        Assertions.assertEquals((Integer)2, df.getRow(2).getInteger(0));

        Assertions.assertEquals((Double)5d, df.getRow(2).getDouble("B"));
        Assertions.assertEquals((Double)5d, df.getRow(2).getDouble(1));
        Assertions.assertEquals("Y", df.getRow(2).getString("C"));
        Assertions.assertEquals("Y", df.getRow(2).getString(2));
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
        Assertions.assertEquals(2, filtered.size());

        filtered = dataFrame.select(
                FilterPredicate.eq("z", true).nor(FilterPredicate.eq("v", true))
        );
        Assertions.assertEquals(1, filtered.size());

        filtered = dataFrame.select(
                FilterPredicate.eq("z", true).xor(FilterPredicate.eq("v", true))
        );
        Assertions.assertEquals(2, filtered.size());

        filtered = dataFrame.select(
                FilterPredicate.in("name", new String[]{"a", "b"}).neg()
        );
        Assertions.assertEquals(2, filtered.size());
        Assertions.assertEquals("c", filtered.getRow(0).getString("name"));
        Assertions.assertEquals("d", filtered.getRow(1).getString("name"));

        filtered = dataFrame.select(
                FilterPredicate.eq("name", "a").neg()
        );
        Assertions.assertEquals(3, filtered.size());
        Assertions.assertEquals("b", filtered.getRow(0).getString("name"));
        Assertions.assertEquals("c", filtered.getRow(1).getString("name"));
        Assertions.assertEquals("d", filtered.getRow(2).getString("name"));

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

        Assertions.assertEquals(dataFrame, dataFrame2);

        DataRow r = dataFrame2.getRow(0);
        r.set("name", "abc");
        dataFrame2.update(r);
        Assertions.assertNotEquals(dataFrame, dataFrame2);

        DataFrame dataFrame3 = dataFrame.copy();
        Assertions.assertEquals(dataFrame, dataFrame3);


    }

    @Test
    public void testNA() throws IOException {

        Assertions.assertEquals(true, Values.NA.isNA(Values.NA));
        Assertions.assertEquals(true, Values.NA.isNA("NA"));
        Assertions.assertEquals(true, Values.NA.isNA(null));
        Assertions.assertEquals(false, Values.NA.isNA(1));
        Assertions.assertEquals(false, Values.NA.isNA("na"));


        String[] header = new String[]{"A", "B", "C", "D"};
        Integer[] col1 = new Integer[]{5, 3, 2, 4, 1};
        Double[] col2 = new Double[]{2d, 3d, null, 5d, 2d};
        String[] col3 = new String[]{"X", "Y", "X", Values.NA.toString(), "X"};
        Boolean[] col4 = new Boolean[]{false, false, true, false, true};
        String csv = createCSV(header, col1, col2, col3, col4);



        CSVReader csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("#")
                .withSeparator('\t')
                .setColumnType("A",Integer.class)
                .setColumnType("B",Double.class)
                .setColumnType("C",String.class)
                .setColumnType("D",Boolean.class)
                .build();
        DataFrame df = DataFrameLoader.load(csv,csvReader);
        List<DataRow> r = df.getRows();
        int i = 0;
        df.addStringColumn("test");
        df.addDoubleColumn("test2");

        for (DataRow row : df.rows()) {
            Assertions.assertEquals(Values.NA, row.get("test"));
            Assertions.assertEquals(Values.NA, row.get("test2"));

            if (i == 2) {
                Assertions.assertEquals(true, row.isNA("B"));
            } else {
                Assertions.assertEquals(false, row.isNA("B"));
            }
            if (i == 3) {
                // Assertions.assertEquals(true, row.isNA("C"));
                Assertions.assertEquals(Values.NA, row.get("C"));
            } else {
                Assertions.assertEquals(false, row.isNA("C"));
            }
            i++;
        }

        DoubleColumn dc = df.getDoubleColumn("B");
        Assertions.assertEquals(col2.length, dc.size());
        Assertions.assertEquals(12d, dc.sum(), 0d);
        Assertions.assertEquals(3d, dc.mean(), 0d);
        Assertions.assertEquals(5d, dc.max(), 0d);
        Assertions.assertEquals(2d, dc.min(), 0d);

    }

    @Test
    public void columnSelectionTest(){
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addDoubleColumn("a")
                .addIntegerColumn("b")
                .addBooleanColumn("c");

        dataFrame.append("A",1d,5, true);
        dataFrame.append("B",2d,4, true);
        dataFrame.append("C",3d,3, false);
        dataFrame.append("D",4d,2, false);

        DataFrame test = dataFrame
                .selectColumns("a")
                .where("b > 3");
        Assertions.assertEquals(1,test.getColumns().size());
        Assertions.assertEquals(2,test.size());
        Assertions.assertEquals(1d,test.getRow(0).get("a"));
        Assertions.assertEquals(2d,test.getRow(1).get("a"));

        test = dataFrame
                .selectColumns(
                        dataFrame.getColumn("name"),
                        dataFrame.getColumn("a"),
                        dataFrame.getColumn("b"))
                .where("c",true);
        Assertions.assertEquals(3,test.getColumns().size());
        Assertions.assertEquals(2,test.size());
        Assertions.assertEquals("A",test.getRow(0).get("name"));
        Assertions.assertEquals("B",test.getRow(1).get("name"));
        Assertions.assertEquals(1d,test.getRow(0).get("a"));
        Assertions.assertEquals(2d,test.getRow(1).get("a"));


        test = dataFrame
                .selectColumns(
                        dataFrame.getColumn("name"),
                        dataFrame.getColumn("c"))
                .where(FilterPredicate.leColumn("b","a"));
        Assertions.assertEquals(2,test.getColumns().size());
        Assertions.assertEquals(2,test.size());
        Assertions.assertEquals("C",test.getRow(0).get("name"));
        Assertions.assertEquals("D",test.getRow(1).get("name"));
        Assertions.assertEquals(false,test.getRow(0).get("c"));
        Assertions.assertEquals(false,test.getRow(1).get("c"));

        test = dataFrame
                .selectColumns("name").allRows();
        Assertions.assertEquals(1,test.getColumns().size());
        Assertions.assertEquals(4,test.size());
        Assertions.assertEquals("A",test.getRow(0).get("name"));
        Assertions.assertEquals("B",test.getRow(1).get("name"));
        Assertions.assertEquals("C",test.getRow(2).get("name"));
        Assertions.assertEquals("D",test.getRow(3).get("name"));
    }

    @Test
    public void checkRowValidity(){
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addDoubleColumn("a")
                .addIntegerColumn("b")
                .addBooleanColumn("c");

        dataFrame.append("A",1,5, true);
        dataFrame.append("B",2d,4, true);

        DataRows rows = dataFrame.getRows();
        DataFrame dataFrameB = rows.toDataFrame();
        Assertions.assertEquals("A", rows.get(0).get("name"));
        dataFrame.sort("a", SortColumn.Direction.Descending);
        Assertions.assertEquals("A", dataFrameB.getValue(0,0));
        Assertions.assertEquals("B", dataFrame.getValue(0,0));

        try {
            Assertions.assertEquals("B", rows.get(0).get("name"));
            fail("Expected a DataFrameRuntimeException to be thrown");
        } catch (DataFrameRuntimeException dataFrameRuntimeException) {
            assertEquals(dataFrameRuntimeException.getMessage(), "row is no longer valid, the dataframe changed since the row object was created");
        }
        rows = dataFrame.getRows();
        Assertions.assertEquals("B", rows.get(0).get("name"));

    }

    @Test
    public void listTest(){
        DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addIntegerColumn("int");

        dataFrame.append("A",1);
        dataFrame.append("B",2);
        dataFrame.append("C",3);

        List<String> names = dataFrame.getStringColumn("name").toList();
        List<Integer> values = dataFrame.getIntegerColumn("int").toList();

        Assertions.assertEquals(3,names.size());
        Assertions.assertEquals(3,values.size());
        Assertions.assertEquals("A",names.get(0));
        Assertions.assertEquals("B",names.get(1));
        Assertions.assertEquals("C",names.get(2));
        Assertions.assertEquals(1,(int)values.get(0));
        Assertions.assertEquals(2,(int)values.get(1));
        Assertions.assertEquals(3,(int)values.get(2));

        names.remove("A");
        values.remove(1);

        Assertions.assertEquals(2,names.size());
        Assertions.assertEquals(2,values.size());

        Assertions.assertEquals(3,dataFrame.getStringColumn("name").size());
        Assertions.assertEquals(3,dataFrame.getIntegerColumn("int").size());


        List<String> namesIm = dataFrame.getStringColumn("name").asList();
        List<Integer> valuesIm = dataFrame.getIntegerColumn("int").asList();

        Assertions.assertEquals(3,namesIm.size());
        Assertions.assertEquals(3,valuesIm.size());
        Assertions.assertEquals("A",namesIm.get(0));
        Assertions.assertEquals("B",namesIm.get(1));
        Assertions.assertEquals("C",namesIm.get(2));
        Assertions.assertEquals(1,(int)valuesIm.get(0));
        Assertions.assertEquals(2,(int)valuesIm.get(1));
        Assertions.assertEquals(3,(int)valuesIm.get(2));

        Assertions.assertEquals(1,valuesIm.indexOf(2));
        Assertions.assertEquals(-1,valuesIm.indexOf(5));

        Object[] oArr = valuesIm.toArray();
        Assertions.assertEquals(3,oArr.length);
        Assertions.assertEquals(2,oArr[1]);

        Integer[] iArr = valuesIm.toArray(new Integer[0]);
        Assertions.assertEquals(3,iArr.length);
        Assertions.assertEquals((Integer)2,iArr[1]);

        iArr = valuesIm.toArray(new Integer[4]);
        Assertions.assertEquals(4,iArr.length);
        Assertions.assertEquals((Integer)2,iArr[1]);
        Assertions.assertEquals(null,iArr[3]);
        try {
            namesIm.add("D");
            namesIm.remove("A");
            Collections.sort(namesIm);

            valuesIm.add(1);
            valuesIm.remove(1);
            Collections.sort(valuesIm);
            fail("Expected an UnsupportedOperationException to be thrown");
        } catch (UnsupportedOperationException e) {
        }
    }

    @Test
    public void testHeadTail(){
        DataFrame df = DataFrame.create()
                .addIntegerColumn("id")
                .addStringColumn("id_str");
        for(int i = 0; i < 100; i++){
            df.append(i,i+"s");
        }
        DataFrame head = df.head();
        DataFrame tail = df.tail();

        Assertions.assertEquals(DefaultDataFrame.DEFAULT_HEAD_SIZE,head.size());
        Assertions.assertEquals(0,head.getValue(0,0));
        Assertions.assertEquals("0s",head.getValue(1,0));

        Assertions.assertEquals(19,head.getValue(0,head.size() - 1));
        Assertions.assertEquals("19s",head.getValue(1,head.size() - 1));

        int tMin = 100 - DefaultDataFrame.DEFAULT_TAIL_SIZE;
        Assertions.assertEquals(tMin,tail.getValue(0,0));
        Assertions.assertEquals(tMin+"s",tail.getValue(1,0));

        Assertions.assertEquals(99,tail.getValue(0,tail.size()- 1));
        Assertions.assertEquals(99+"s",tail.getValue(1,tail.size() - 1));

        Assertions.assertEquals(DefaultDataFrame.DEFAULT_HEAD_SIZE,tail.size());

        for(int i = 0; i  < DefaultDataFrame.DEFAULT_HEAD_SIZE;i++){
            Assertions.assertEquals(Integer.valueOf(i),head.getRow(i).getInteger(0));
            Assertions.assertEquals(i+"s",head.getRow(i).getString(1));
        }

        for(int i = 0; i  < DefaultDataFrame.DEFAULT_TAIL_SIZE;i++){
            int idx = df.size() - DefaultDataFrame.DEFAULT_TAIL_SIZE + i;
            Assertions.assertEquals(Integer.valueOf(idx),tail.getRow(i).getInteger(0));
            Assertions.assertEquals(idx+"s",tail.getRow(i).getString(1));
        }
    }

    @Test
    public void testColumnSelectErrors(){
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> {
            DataFrame df = NoConstructorDataFrame.create();
            df.append("A",1d,5, true);
            df.selectColumns("name").allRows();
        });
    }
    private static class NoConstructorDataFrame extends DefaultDataFrame{
        private NoConstructorDataFrame(){}
        public static NoConstructorDataFrame create(){
            return new NoConstructorDataFrame();
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
