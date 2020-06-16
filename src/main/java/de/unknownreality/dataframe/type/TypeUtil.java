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

package de.unknownreality.dataframe.type;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.type.impl.*;

import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Alex on 04.06.2015.
 */
public class TypeUtil {
    private final static Map<Class<?>, ValueType<?>> typeMap = new ConcurrentHashMap<>();

    static {
        init();
    }

    private static Map<Class<?>, ValueType<?>> getTypeMap() {
        return typeMap;
    }

    /**
     * Adds the default types
     */
    private static void init() {
        typeMap.put(String.class, new StringType());

        typeMap.put(Double.class, new DoubleType());

        typeMap.put(Integer.class, new IntegerType());

        typeMap.put(Float.class, new FloatType());

        typeMap.put(Long.class, new LongType());

        typeMap.put(Boolean.class, new BooleanType());

        typeMap.put(Short.class, new ShortType());

        typeMap.put(Character.class, new CharacterType());

        typeMap.put(Byte.class, new ByteType());

    }


    /**
     * Parses a String into an array. The corresponding value type for the array type is used.
     *
     * @param cl  array class (String[].class)
     * @param x   String to parse
     * @param <T> type of array class
     * @return parsed array object
     * @throws ParseException thrown if the string can not be parsed
     */
    private static <T> T parseArray(Class<T> cl, String x) throws ParseException {
        ValueType<?> p = getTypeMap().get(cl.getComponentType());
        Class<?> cc = cl.getComponentType();
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
     * @throws ParseException             thrown if the string can not be parsed
     * @throws ValueTypeNotFoundException thrown if no type is found for the object type
     */
    public static <T> T parse(Class<T> cl, String x) throws ParseException, ValueTypeNotFoundException {
        if ((cl.isArray() && !getTypeMap().containsKey(cl.getComponentType())) && !getTypeMap().containsKey(cl)) {
            throw new ValueTypeNotFoundException(cl);
        }
        try {
            if (cl.isArray()) {
                return parseArray(cl, x);
            }
            ValueType<T> p = getType(cl);
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
    public static boolean typeExists(Class<?> cl) {
        return getTypeMap().get(cl) != null;
    }

    /**
     * Returns a parser for the input class
     *
     * @param cl  input class
     * @param <T> type of class
     * @return type for input class
     * @throws ValueTypeNotFoundException thrown if no parser is found
     */
    @SuppressWarnings("unchecked")
    public static <T> ValueType<T> getType(Class<T> cl) throws ValueTypeNotFoundException {
        ValueType<?> type = getTypeMap().get(cl);
        if (type == null) {
            throw new ValueTypeNotFoundException(cl);
        }
        return (ValueType<T>) type;
    }

    /**
     * Returns a value type for the input class or null if no type is found
     *
     * @param cl  input class
     * @param <T> type of class
     * @return type for input class
     */
    public static <T> ValueType<T> findTypeOrNull(Class<T> cl) {
        try {
            return getType(cl);
        } catch (ValueTypeNotFoundException ignored) {
        }
        return null;
    }

    /**
     * Returns a value type for the input class throws a runtime exception if no type is found
     *
     * @param cl  input class
     * @param <T> type of class
     * @return type for input class
     */
    public static <T> ValueType<T> findTypeOrThrow(Class<T> cl) {
        try {
            return getType(cl);
        } catch (ValueTypeNotFoundException e) {
            throw new DataFrameRuntimeException("value type not found", e);
        }
    }

    /**
     * Parses a String into an object of input type.
     * returns null if no type is found or the String can't be parsed.
     *
     * @param cl  Class of resulting object
     * @param x   String to parse
     * @param <T> type of resulting object
     * @return parsed Object
     */
    public static <T> T parseOrNull(Class<T> cl, String x) {
        if ((cl.isArray() && !getTypeMap().containsKey(cl.getComponentType())) && !getTypeMap().containsKey(cl)) {
            return null;
        }
        try {
            if (cl.isArray()) {
                return parseArray(cl, x);
            }
            ValueType<T> p = getType(cl);
            return p.parse(x);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Adds a parser for the input class
     *
     * @param c   class of parser
     * @param p   value type
     * @param <T> type of class
     */
    public static <T> void addType(Class<T> c, ValueType<T> p) {
        getTypeMap().put(c, p);
    }
}
