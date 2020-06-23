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

import de.unknownreality.dataframe.type.TypeUtil;
import de.unknownreality.dataframe.type.ValueTypeNotFoundException;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.fail;

public class ParserTest {
    @Test
    public void testParserUtil() throws ParseException, ValueTypeNotFoundException {
        Assert.assertEquals((Integer) 1, TypeUtil.parse(Integer.class, "1"));

        Assert.assertEquals((Double) 1.5, TypeUtil.parse(Double.class, "1.5"));


        Assert.assertEquals(true, TypeUtil.parse(Boolean.class, "t"));
        Assert.assertEquals(false, TypeUtil.parse(Boolean.class, "f"));
        Assert.assertEquals(true, TypeUtil.parse(Boolean.class, "T"));
        Assert.assertEquals(false, TypeUtil.parse(Boolean.class, "F"));


        try {
            TypeUtil.parse(Boolean.class, "x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        try {
            TypeUtil.parse(Double.class, "x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        Assert.assertNull(TypeUtil.parseOrNull(Double.class, "x"));
        Assert.assertNull(TypeUtil.parseOrNull(ParserTest.class, "x"));


        Assert.assertTrue(TypeUtil.typeExists(Double.class));
        Assert.assertFalse(TypeUtil.typeExists(ParserTest.class));

        try {
            TypeUtil.getType(ParserTest.class);
            fail("Expected a ParseException to be thrown");
        } catch (ValueTypeNotFoundException parseException) {
        }


        try {
            TypeUtil.getType(ParserTest.class);
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ValueTypeNotFoundException parseException) {
        }

        try {
            TypeUtil.parse(ParserTest.class, "x");
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ParseException parseException) {
        }

        Integer[] integers = TypeUtil.parse(Integer[].class, "1,2,3");
        Assert.assertEquals(3,integers.length);
        Assert.assertEquals((Integer)1,integers[0]);
        Assert.assertEquals((Integer)2,integers[1]);
        Assert.assertEquals((Integer)3,integers[2]);

        try {
            TypeUtil.parse(ParserTest[].class, "x");
            fail("Expected a ValueTypeNotFoundException to be thrown");
        } catch (ValueTypeNotFoundException parseException) {
        }
    }
}
