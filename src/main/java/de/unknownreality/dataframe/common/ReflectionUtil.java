/*
 *
 *  * Copyright (c) 2019-2026 Alexander Grün
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Reflection helpers that report failures as {@link DataFrameRuntimeException}
 */
public final class ReflectionUtil {

    private ReflectionUtil() {
    }

    /**
     * Creates an instance of a class using its no-argument constructor.
     * The constructor does not need to be public.
     *
     * @param cl  class to instantiate
     * @param <T> type of the instance
     * @return new instance
     * @throws DataFrameRuntimeException if the class can not be instantiated.
     */
    public static <T> T newInstance(Class<T> cl) {
        if (cl == null) {
            throw new DataFrameRuntimeException("class is null, can not create instance");
        }
        Constructor<T> constructor;
        try {
            constructor = cl.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new DataFrameRuntimeException(
                    String.format("class '%s' has no no-argument constructor", cl.getName()), e);
        }
        try {
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new DataFrameRuntimeException(
                    String.format("constructor of class '%s' threw an exception", cl.getName()), cause);
        } catch (InstantiationException e) {
            throw new DataFrameRuntimeException(
                    String.format("class '%s' can not be instantiated", cl.getName()), e);
        } catch (IllegalAccessException | RuntimeException e) {
            throw new DataFrameRuntimeException(
                    String.format("no access to the no-argument constructor of class '%s'", cl.getName()), e);
        }
    }
}
