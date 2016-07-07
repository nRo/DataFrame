package de.unknownreality.dataframe.common;

/**
 * Created by Alex on 07.07.2016.
 */
public class NumberUtil {
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

    @SuppressWarnings("unchecked")
    public static <T extends Number> T convert(Number n, Class<T> cl) {
        if (cl == Double.class) {
            return (T) new Double(n.doubleValue());
        } else if (cl == Integer.class) {
            return (T) new Integer(n.intValue());
        } else if (cl == Float.class) {
            return (T) new Float(n.floatValue());
        } else if (cl == Long.class) {
            return (T) new Long(n.longValue());
        } else if (cl == Short.class) {
            return (T) new Short(n.shortValue());
        } else if (cl == Byte.class) {
            return (T) new Byte(n.byteValue());
        }
        return null;
    }
}
