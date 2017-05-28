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

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.column.BooleanColumn;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.column.StringColumn;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.filter.compile.PredicateCompiler;
import de.unknownreality.dataframe.filter.compile.PredicateCompilerException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by Alex on 12.03.2016.
 */
public class PredicateParserTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void test() {

        DataFrame dataFrame = new DataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new BooleanColumn("z"));
        dataFrame.addColumn(new StringColumn("r"));


        dataFrame.append("a",1d,5,true,"abc123");
        dataFrame.append("b",2d,4,true,"abc/123");
        dataFrame.append("c",3d,3,false,"abc");
        dataFrame.append("d",4d,2,false,"123");

        FilterPredicate predicate = PredicateCompiler.compile("(name != 'a') AND (x < 3)");
        DataFrame filtered = dataFrame.select(predicate);
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));

        filtered = dataFrame.select("((((name != 'a')) AND (x < 3.5))) OR((y == 2))");
        Assert.assertEquals(3,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));
        Assert.assertEquals("c",filtered.getRow(1).getString("name"));
        Assert.assertEquals("d",filtered.getRow(2).getString("name"));


        filtered = dataFrame.select("r ~= /[a-z]+.+/");
        Assert.assertEquals(3,filtered.size());

        filtered = dataFrame.select("r ~= /[a-z]+\\/[0-9]+/");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));


        filtered = dataFrame.select("^(name == 'a')");
        Assert.assertEquals(3,filtered.size());


        filtered = dataFrame.select("^((name == 'a') OR (x > 2))");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));


        filtered = dataFrame.select("z == true");
        Assert.assertEquals(2,filtered.size());
    }

    @Test(expected=PredicateCompilerException.class)
    public void testWrongOperation() {
        PredicateCompiler.compile("((name != 'a') X (x < 3.5)) OR (y == 2)");
    }

    @Test(expected=PredicateCompilerException.class)
    public void testBrackets() {
        PredicateCompiler.compile("((name != 'a') AND (x < 3.5) OR (y == 2)");
    }

    @Test(expected=PredicateCompilerException.class)
    public void testValue() {
        PredicateCompiler.compile("((name != 'a') AND (x < a) OR (y == 2)");
    }
}
