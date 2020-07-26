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

package de.unknownreality.dataframe.value;

import de.unknownreality.dataframe.type.DataFrameTypeManager;
import de.unknownreality.dataframe.type.ValueTypeNotFoundException;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.fail;

public class ParserTest {
    @Test
    public void testParserUtil() throws ParseException, ValueTypeNotFoundException {
        Assert.assertEquals((Integer) 1, DataFrameTypeManager.get().parse(Integer.class, "1"));

        Assert.assertEquals((Double) 1.5, DataFrameTypeManager.get().parse(Double.class, "1.5"));


        Assert.assertEquals(true, DataFrameTypeManager.get().parse(Boolean.class, "t"));
        Assert.assertEquals(false, DataFrameTypeManager.get().parse(Boolean.class, "f"));
        Assert.assertEquals(true, DataFrameTypeManager.get().parse(Boolean.class, "T"));
        Assert.assertEquals(false, DataFrameTypeManager.get().parse(Boolean.class, "F"));


        try {
            DataFrameTypeManager.get().parse(Boolean.class, "x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        try {
            DataFrameTypeManager.get().parse(Double.class, "x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        Assert.assertNull(DataFrameTypeManager.get().parseOrNull(Double.class, "x"));
        Assert.assertNull(DataFrameTypeManager.get().parseOrNull(ParserTest.class, "x"));


        Assert.assertTrue(DataFrameTypeManager.get().typeExists(Double.class));
        Assert.assertFalse(DataFrameTypeManager.get().typeExists(ParserTest.class));

        try {
            DataFrameTypeManager.get().getValueType(ParserTest.class);
            fail("Expected a ParseException to be thrown");
        } catch (ValueTypeNotFoundException parseException) {
        }


        try {
            DataFrameTypeManager.get().getValueType(ParserTest.class);
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ValueTypeNotFoundException parseException) {
        }

        try {
            DataFrameTypeManager.get().parse(ParserTest.class, "x");
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ParseException parseException) {
        }

        Integer[] integers = DataFrameTypeManager.get().parse(Integer[].class, "1,2,3");
        Assert.assertEquals(3,integers.length);
        Assert.assertEquals((Integer)1,integers[0]);
        Assert.assertEquals((Integer)2,integers[1]);
        Assert.assertEquals((Integer)3,integers[2]);

        try {
            DataFrameTypeManager.get().parse(ParserTest[].class, "x");
            fail("Expected a ValueTypeNotFoundException to be thrown");
        } catch (ValueTypeNotFoundException parseException) {
        }
    }
}
