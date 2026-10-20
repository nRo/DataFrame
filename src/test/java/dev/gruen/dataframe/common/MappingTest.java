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

package dev.gruen.dataframe.common;

import dev.gruen.dataframe.DataFrame;
import dev.gruen.dataframe.DataRow;
import dev.gruen.dataframe.DefaultDataFrame;
import dev.gruen.dataframe.Values;
import dev.gruen.dataframe.column.BooleanColumn;
import dev.gruen.dataframe.column.DoubleColumn;
import dev.gruen.dataframe.column.IntegerColumn;
import dev.gruen.dataframe.column.StringColumn;
import dev.gruen.dataframe.common.mapping.DataMapper;
import dev.gruen.dataframe.common.mapping.MappedColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Created by Alex on 28.06.2017.
 */
public class MappingTest {
    @Test
    public void testMapping() {
        DataFrame dataFrame = new DefaultDataFrame();

        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new BooleanColumn("z"));
        dataFrame.addColumn(new BooleanColumn("v"));
        dataFrame.addColumn(new StringColumn("r"));
        dataFrame.addColumn(new IntegerColumn("n"));


        dataFrame.append("a", 1d, 5, true, true, "abc123", 1);
        dataFrame.append("b", 2d, 4, true, false, "abc/123", null);
        dataFrame.append("c", 3d, 3, false, true, "abc", Values.NA);
        dataFrame.append("d", 4d, 2, false, false, "123", 1);

        List<TestObject> mappedObject = dataFrame.map(TestObject.class);

        Assertions.assertEquals(dataFrame.size(), mappedObject.size());
        for (int i = 0; i < dataFrame.size(); i++) {
            DataRow row = dataFrame.getRow(i);
            Assertions.assertEquals(row.getString("name"), mappedObject.get(i).getName());
            Assertions.assertEquals(row.getDouble("x"), mappedObject.get(i).getX());
            Assertions.assertEquals(row.getDouble("y"), mappedObject.get(i).getY());
        }

        int i = 0;
        for(TestObject testObject : DataMapper.mapEach(dataFrame,TestObject.class)){
            DataRow row = dataFrame.getRow(i);
            Assertions.assertEquals(row.getString("name"),testObject.getName());
            Assertions.assertEquals(row.getDouble("x"), testObject.getX());
            Assertions.assertEquals(row.getDouble("y"), testObject.getY());
            i++;
        }
    }

    public static class TestObject {
        @MappedColumn(header = "name")
        private String name;

        @MappedColumn
        private Double x;

        @MappedColumn(index = 2)
        private Double y;

        public Double getX() {
            return x;
        }

        public void setY(Double y) {
            this.y = y;
        }

        public void setX(Double x) {
            this.x = x;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Double getY() {
            return y;
        }
    }
}
