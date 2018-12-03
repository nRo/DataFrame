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
            ParserUtil.parse(Double.class,"x");
            fail("Expected a ParseException to be thrown");
        } catch (ParseException parseException) {
        }

        Assert.assertEquals(null,ParserUtil.parseOrNull(Double.class,"x"));
        Assert.assertEquals(null,ParserUtil.parseOrNull(ParserTest.class,"x"));



        Assert.assertEquals(true, ParserUtil.hasParser(Double.class));
        Assert.assertEquals(false, ParserUtil.hasParser(ParserTest.class));

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
