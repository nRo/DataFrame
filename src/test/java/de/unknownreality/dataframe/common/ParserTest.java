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

package de.unknownreality.dataframe.common;

import de.unknownreality.dataframe.common.parser.ParserNotFoundException;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

import static org.junit.Assert.fail;

public class ParserTest {
    @Test
    public void testParserUtil() throws ParseException, ParserNotFoundException {
        Assert.assertEquals((Integer)1, ParserUtil.parse(Integer.class,"1"));

        Assert.assertEquals((Double)1.5, ParserUtil.parse(Double.class,"1.5"));



        Assert.assertEquals(true, ParserUtil.parse(Boolean.class,"t"));
        Assert.assertEquals(false, ParserUtil.parse(Boolean.class,"f"));
        Assert.assertEquals(true, ParserUtil.parse(Boolean.class,"T"));
        Assert.assertEquals(false, ParserUtil.parse(Boolean.class,"F"));


        try {
            ParserUtil.parse(Boolean.class,"x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        try {
            ParserUtil.parse(Double.class, "x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        Assert.assertNull(ParserUtil.parseOrNull(Double.class, "x"));
        Assert.assertNull(ParserUtil.parseOrNull(ParserTest.class, "x"));


        Assert.assertTrue(ParserUtil.hasParser(Double.class));
        Assert.assertFalse(ParserUtil.hasParser(ParserTest.class));

        try {
            ParserUtil.getParser(ParserTest.class);
            fail("Expected a ParseException to be thrown");
        } catch (ParserNotFoundException parseException) {
        }


        try {
            ParserUtil.getParser(ParserTest.class);
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ParserNotFoundException parseException) {
        }

        try {
            ParserUtil.parse(ParserTest.class,"x");
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ParseException parseException) {
        }

        Integer[] integers = ParserUtil.parse(Integer[].class,"1,2,3");
        Assert.assertEquals(3,integers.length);
        Assert.assertEquals((Integer)1,integers[0]);
        Assert.assertEquals((Integer)2,integers[1]);
        Assert.assertEquals((Integer)3,integers[2]);

        try {
            ParserUtil.parse(ParserTest[].class,"x");
            fail("Expected a ParserNotFoundException to be thrown");
        } catch (ParserNotFoundException parseException) {
        }
    }
}
