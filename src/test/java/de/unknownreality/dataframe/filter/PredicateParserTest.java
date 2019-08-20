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

package de.unknownreality.dataframe.filter;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DefaultDataFrame;
import de.unknownreality.dataframe.Values;
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

        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new BooleanColumn("z"));
        dataFrame.addColumn(new BooleanColumn("v"));
        dataFrame.addColumn(new StringColumn("r"));
        dataFrame.addColumn(new IntegerColumn("n"));


        dataFrame.append("a",1d,5,true,true,"abc123",1);
        dataFrame.append("b",2d,4,true,false,"abc/123",null);
        dataFrame.append("c",3d,3,false,true,"abc", Values.NA);
        dataFrame.append("d",4d,2,false,false,"123",1);



        FilterPredicate predicate = PredicateCompiler.compile("(.name != 'a') AND (x < 3)");
        DataFrame filtered = dataFrame.select(predicate);
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));

        filtered = dataFrame.select("(name != 'a' && z) OR (name != 'd' && !z)");
        Assert.assertEquals(2,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));
        Assert.assertEquals("c",filtered.getRow(1).getString("name"));


        filtered = dataFrame.select("x >= 1");
        Assert.assertEquals(4,filtered.size());

        filtered = dataFrame.select("x > 1");
        Assert.assertEquals(3,filtered.size());

        filtered = dataFrame.select("x < 1.5");
        Assert.assertEquals(1,filtered.size());

        filtered = dataFrame.select("x <= 1");
        Assert.assertEquals(1,filtered.size());


        filtered = dataFrame.select("r >= '123'");
        Assert.assertEquals(4,filtered.size());

        filtered = dataFrame.select("r > '123'");
        Assert.assertEquals(3,filtered.size());

        filtered = dataFrame.select("r < 'xyz'");
        Assert.assertEquals(4,filtered.size());

        filtered = dataFrame.select("r <= '123'");
        Assert.assertEquals(1,filtered.size());

        filtered = dataFrame.select("((((.name != 'a')) AND (x < 3.5))) OR((y == 2))");
        Assert.assertEquals(3,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));
        Assert.assertEquals("c",filtered.getRow(1).getString("name"));
        Assert.assertEquals("d",filtered.getRow(2).getString("name"));


        filtered = dataFrame.select("r ~= /[a-z]+.+/");
        Assert.assertEquals(3,filtered.size());

        filtered = dataFrame.select(".r ~= /[a-z]+\\/[0-9]+/");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));


        filtered = dataFrame.select("!(name == 'a')");
        Assert.assertEquals(3,filtered.size());


        filtered = dataFrame.select("!((name == 'a') OR (.x > 2))");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));


        filtered = dataFrame.select("z == true");
        Assert.assertEquals(2,filtered.size());

        filtered = dataFrame.select("(.z == true) XOR (v == true)");
        Assert.assertEquals(2,filtered.size());

        DataFrame filtered2 = dataFrame.select(".z XOR v");
        Assert.assertEquals(2,filtered2.size());
        Assert.assertEquals(filtered,filtered2);


        filtered = dataFrame.select("(z == true) NOR (v == true)");
        Assert.assertEquals(1,filtered.size());
        filtered2 = dataFrame.select("z NOR v");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals(filtered,filtered2);



        filtered = dataFrame.select(".z == .v");
        Assert.assertEquals(2,filtered.size());
        Assert.assertEquals("a",filtered.getRow(0).getString("name"));
        Assert.assertEquals("d",filtered.getRow(1).getString("name"));

        filtered = dataFrame.select(".z != .v");
        Assert.assertEquals(2,filtered.size());
        Assert.assertEquals("b",filtered.getRow(0).getString("name"));
        Assert.assertEquals("c",filtered.getRow(1).getString("name"));

        filtered = dataFrame.select(".z && .v");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("a",filtered.getRow(0).getString("name"));

        filtered = dataFrame.select("z || v");
        Assert.assertEquals(3,filtered.size());

        filtered = dataFrame.select("n == null");
        Assert.assertEquals(2,filtered.size());
    }


    @Test
    public void likeTest(){
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new IntegerColumn("value"));
        dataFrame.append("A B C",1);
        dataFrame.append("A B C",2);
        dataFrame.append("A D C",3);
        dataFrame.append("C D E",4);
        dataFrame.append("E F C",5);
        dataFrame.append("A F C",6);
        dataFrame.append("G C H",7);
        DataFrame filtered = dataFrame.select("name LIKE 'A B C'");
        Assert.assertEquals(2,filtered.size());

        filtered = dataFrame.select("name LIKE 'A _ C'");
        Assert.assertEquals(4,filtered.size());

        filtered = dataFrame.select("name LIKE '%C'");
        Assert.assertEquals(5,filtered.size());

        filtered = dataFrame.select("name LIKE 'A%'");
        Assert.assertEquals(4,filtered.size());

        filtered = dataFrame.select("name LIKE 'E%'");
        Assert.assertEquals(1,filtered.size());

        filtered = dataFrame.select("name LIKE '% _ C'");
        Assert.assertEquals(5,filtered.size());

        filtered = dataFrame.select("name LIKE '% _ %'");
        Assert.assertEquals(7,filtered.size());

        filtered = dataFrame.select("name LIKE '%D%'");
        Assert.assertEquals(2,filtered.size());

        filtered = dataFrame.select("name LIKE '%D%'");
        Assert.assertEquals(2,filtered.size());

        filtered = dataFrame.select("name LIKE '%C%'");
        Assert.assertEquals(7,filtered.size());

        filtered = dataFrame.select("name LIKE 'A'");
        Assert.assertEquals(0,filtered.size());

        filtered = dataFrame.select("name LIKE '_'");
        Assert.assertEquals(0,filtered.size());

        filtered = dataFrame.select("name LIKE '%AAAAAAAA'");
        Assert.assertEquals(0,filtered.size());

        filtered = dataFrame.select("name LIKE '%AAAAAAAA%'");
        Assert.assertEquals(0,filtered.size());

        filtered = dataFrame.select("name LIKE 'AAAAAAAA%'");
        Assert.assertEquals(0,filtered.size());
    }

    @Test
    public void numberTest(){
        DataFrame dataFrame = new DefaultDataFrame();
        dataFrame.addColumn(new StringColumn("name"));
        dataFrame.addColumn(new DoubleColumn("x"));
        dataFrame.addColumn(new IntegerColumn("y"));
        dataFrame.addColumn(new StringColumn("z"));

        dataFrame.append("a",1d,5,"10");
        dataFrame.append("b",2.3d,4,"2");
        dataFrame.append("c",3.5d,3,null);
        dataFrame.append("d",null,5,"4.5");
        dataFrame.append("e",4.5,5,"5.4321");


        DataFrame filtered = dataFrame.select("z == 10");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("a",filtered.getRow(0).getString("name"));

        filtered = dataFrame.select("z > 6");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("a",filtered.getRow(0).getString("name"));

        filtered = dataFrame.select("z >= 4.5");
        Assert.assertEquals(3,filtered.size());
        Assert.assertEquals("a",filtered.getRow(0).getString("name"));
        Assert.assertEquals("d",filtered.getRow(1).getString("name"));
        Assert.assertEquals("e",filtered.getRow(2).getString("name"));

        filtered = dataFrame.select("z >= 4.5 && z < 5.5");
        Assert.assertEquals(2,filtered.size());
        Assert.assertEquals("d",filtered.getRow(0).getString("name"));
        Assert.assertEquals("e",filtered.getRow(1).getString("name"));

        filtered = dataFrame.select("z == 5.4321");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("e",filtered.getRow(0).getString("name"));

        filtered = dataFrame.select("z == null");
        Assert.assertEquals(1,filtered.size());
        Assert.assertEquals("c",filtered.getRow(0).getString("name"));
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

    @Test(expected=PredicateCompilerException.class)
    public void testColValue() {
        PredicateCompiler.compile("((name != .a) AND (x < a) OR (y == 2)");
    }
}
