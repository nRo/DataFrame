/*
 *
 *  * Copyright (c) 2017 Alexander Grün
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

package de.unknownreality.dataframe.common.parser;

import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Alex on 04.06.2015.
 */
public class ParserUtil {
    private final static Map<Class<?>, Parser<?>> parserMap = new ConcurrentHashMap<>();
    static{
        init();
    }

    private static Map<Class<?>, Parser<?>> getParserMap() {
        return parserMap;
    }

    /**
     * Adds the default parsers
     */
    private static void init() {
        parserMap.put(String.class, new Parser<String>() {
            @Override
            public String parse(String s) {
                return s;
            }
        });

        parserMap.put(Double.class, new Parser<Double>() {
            @Override
            public Double parse(String s) {
                return Double.parseDouble(s);
            }
        });


        parserMap.put(Integer.class, new Parser<Integer>() {
            @Override
            public Integer parse(String s) {
                return Integer.parseInt(s);
            }
        });


        parserMap.put(Float.class, new Parser<Float>() {
            @Override
            public Float parse(String s) {
                return Float.parseFloat(s);
            }
        });

        parserMap.put(Long.class, new Parser<Long>() {
            @Override
            public Long parse(String s) {
                return Long.parseLong(s);
            }
        });

        parserMap.put(Boolean.class, new Parser<Boolean>() {
            @Override
            public Boolean parse(String s) throws ParseException {
                if (s == null || !(
                        "false".equals(s.toLowerCase())
                                || "true".equals(s.toLowerCase())
                                || "f".equals(s.toLowerCase())
                                || "t".equals(s.toLowerCase())
                )) {
                    throw new ParseException(String.format("illegal boolean value: %s", s), 0);
                }
                return Boolean.parseBoolean(s);
            }
        });

        parserMap.put(Short.class, new Parser<Short>() {
            @Override
            public Short parse(String s) {
                return Short.parseShort(s);
            }
        });

        parserMap.put(Character.class, new Parser<Character>() {
            @Override
            public Character parse(String s) {
                return s.charAt(0);
            }
        });

        parserMap.put(Byte.class, new Parser<Byte>() {
            @Override
            public Byte parse(String s) {
                return Byte.parseByte(s);
            }
        });

    }


    /**
     * Parses a String into an array. The corresponding parser for the array type is used.
     *
     * @param cl  array class (String[].class)
     * @param x   String to parse
     * @param <T> type of array class
     * @return parsed array object
     * @throws ParseException thrown if the string can not be parsed
     */
    private static <T> T parseArray(Class<T> cl, String x) throws ParseException {
        Parser p = getParserMap().get(cl.getComponentType());
        Class cc = cl.getComponentType();
        String[] vals = x.split("[;,|]");
        Object r = Array.newInstance(cc, vals.length);
        for (int i = 0; i < vals.length; i++) {
            Array.set(r, i, p.parse(vals[i]));
        }
        return cl.cast(r);
    }


    /**
     * Parses a String into an Object of a defined type
     *
     * @param cl  class of the returned object
     * @param x   String to parse
     * @param <T> type of the returned object
     * @return parsed object
     * @throws ParseException          thrown if the string can not be parsed
     * @throws ParserNotFoundException thrown if no parser is found for the object type
     */
    @SuppressWarnings("unchecked")
    public static <T> T parse(Class<T> cl, String x) throws ParseException, ParserNotFoundException {
        if ((cl.isArray() && !getParserMap().containsKey(cl.getComponentType())) && !getParserMap().containsKey(cl)) {
            throw new ParserNotFoundException(cl);
        }
        try {
            if (cl.isArray()) {
                return parseArray(cl, x);
            }
            Parser<T> p = getParser(cl);
            return p.parse(x);
        } catch (Exception e) {
            throw new ParseException("error parsing '" + x + "' to " + cl.getName(), 0);
        }
    }


    /**
     * Checks if a parser for the class is available
     *
     * @param cl input class
     * @return true if parser is available
     */
    public static boolean hasParser(Class<?> cl) {
        return getParserMap().get(cl) != null;
    }

    /**
     * Returns a parser for the input class
     *
     * @param cl  input class
     * @param <T> type of class
     * @return parser for input class
     * @throws ParserNotFoundException thrown if no parser is found
     */
    @SuppressWarnings("unchecked")
    public static <T> Parser<T> getParser(Class<T> cl) throws ParserNotFoundException {
        Parser parser = getParserMap().get(cl);
        if (parser == null) {
            throw new ParserNotFoundException(cl);
        }
        return (Parser<T>) parser;
    }

    /**
     * Returns a parser for the input class or null if no parser is found
     *
     * @param cl  input class
     * @param <T> type of class
     * @return parser for input class
     */
    public static <T> Parser<T> findParserOrNull(Class<T> cl) {
        try {
            return getParser(cl);
        } catch (ParserNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Parses a String into an object of input type.
     * returns null if no parser is found or the String can't be parsed.
     *
     * @param cl  Class of resulting object
     * @param x   String to parse
     * @param <T> type of resulting object
     * @return parsed Object
     */
    public static <T> T parseOrNull(Class<T> cl, String x) {
        if ((cl.isArray() && !getParserMap().containsKey(cl.getComponentType())) && !getParserMap().containsKey(cl)) {
            return null;
        }
        try {
            if (cl.isArray()) {
                return parseArray(cl, x);
            }
            Parser<T> p = getParser(cl);
            return p.parse(x);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Adds a parser for the input class
     *
     * @param c   class of parser
     * @param p   parser
     * @param <T> type of class
     */
    public static <T> void addParser(Class<T> c, Parser<T> p) {
        getParserMap().put(c, p);
    }
}
