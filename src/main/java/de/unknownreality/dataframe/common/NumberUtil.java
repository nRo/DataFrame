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

import de.unknownreality.dataframe.DataFrameRuntimeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

/**
 * Created by Alex on 07.07.2016.
 */
public class NumberUtil {
    //default Number format (US locale)
    private static NumberFormat NUMBER_FORMAT = NumberFormat.getInstance(Locale.US);

    private NumberUtil(){}
    public static <T extends Number> T add(Number a, Number b, Class<T> cl) {
        return convert(a.doubleValue() + b.doubleValue(), cl);
    }

    public static <T extends Number> T subtract(Number a, Number b, Class<T> cl) {
        return convert(a.doubleValue() - b.doubleValue(), cl);
    }

    public static <T extends Number> T multiply(Number a, Number b, Class<T> cl) {
        return convert(a.doubleValue() * b.doubleValue(), cl);
    }

    public static <T extends Number> T divide(Number a, Number b, Class<T> cl) {
        return convert(a.doubleValue() / b.doubleValue(), cl);
    }


    public static boolean le(Number a, Number b) {
        return (compare(a, b) <= 0);
    }

    public static boolean lt(Number a, Number b) {
        return (compare(a, b) < 0);
    }

    public static boolean ge(Number a, Number b) {
        return (compare(a, b) >= 0);
    }

    public static boolean gt(Number a, Number b) {
        return (compare(a, b) > 0);
    }


    public static boolean eq(Number a, Number b) {
        return (compare(a, b) == 0);
    }


    public static Number max(Number a, Number b) {
        int c = compare(a, b);
        if (c > 0) {
            return a;
        }
        return b;
    }

    public static Number min(Number a, Number b) {
        int c = compare(a, b);
        if (c < 0) {
            return a;
        }
        return b;
    }

    public static int compare(Number a, Number b) {
        if (isSpecialNumber(a) || isSpecialNumber(b))
            return Double.compare(a.doubleValue(), b.doubleValue());
        else
            return toBigDecimal(a).compareTo(toBigDecimal(b));
    }

    public static boolean isSpecialNumber(Number number) {
        if(number instanceof Double && !Double.isFinite((Double)number)){
            return true;
        }
        return number instanceof Float && !Float.isFinite((Float)number);
    }

    public static BigDecimal toBigDecimal(Number number) {
        if (number instanceof BigDecimal)
            return (BigDecimal) number;
        if (number instanceof BigInteger)
            return new BigDecimal((BigInteger) number);
        if (number instanceof Byte || number instanceof Short
                || number instanceof Integer || number instanceof Long)
            return BigDecimal.valueOf(number.longValue());
        if (number instanceof Float || number instanceof Double)
            return BigDecimal.valueOf(number.doubleValue());
        try {
            return new BigDecimal(number.toString());
        } catch (final NumberFormatException e) {
            throw new DataFrameRuntimeException("\"" + number + "\" of class " + number.getClass().getName() + " can not be converted to String", e);
        }
    }

    public static Number parseNumber(String numberStr) throws ParseException {
        return NUMBER_FORMAT.parse(numberStr);
    }
    public static Number parseNumberOrNull(String numberStr) {
        try {
            return parseNumber(numberStr);
        } catch (ParseException e) {
            return null;
        }
    }

    public static boolean isFloatOrDouble(Number number){
        return number instanceof Float || number instanceof Double;
    }
    public static String toString(Number number){
        return NUMBER_FORMAT.format(number);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Number> T convert(Number n, Class<T> cl) {
        if(n.getClass() == cl){
            return (T)n;
        }
        if (cl == Double.class) {
            return (T) Double.valueOf(n.doubleValue());
        } else if (cl == Integer.class) {
            return (T) Integer.valueOf(n.intValue());
        } else if (cl == Float.class) {
            return (T) Float.valueOf(n.floatValue());
        } else if (cl == Long.class) {
            return (T) Long.valueOf(n.longValue());
        } else if (cl == Short.class) {
            return (T) Short.valueOf(n.shortValue());
        } else if (cl == Byte.class) {
            return (T) Byte.valueOf(n.byteValue());
        }
        return null;
    }
}
