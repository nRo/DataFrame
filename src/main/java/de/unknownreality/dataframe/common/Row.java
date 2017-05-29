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

package de.unknownreality.dataframe.common;

/**
 * Created by Alex on 10.03.2016.
 */
public interface Row<V,H> {

    /**
     * Returns an entity using head name as a specified type.
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name of column
     * @param cl         class of resulting entity
     * @param <T>        type of entity
     * @return entity as specified type
     */
    <T> T get(H headerName, Class<T> cl);


    /**
     * Returns an entity using head name as a specified type.
     * This method returns <tt>null</tt> if anything goes wrong.
     *
     * @param headerName header name of column
     * @param cl         class of resulting entity
     * @param <T>        type of entity
     * @return entity as specified type
     */
    <T> T getOrNull(H headerName, Class<T> cl);


    /**
     * Returns the entity as a specified type
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index in this row
     * @param cl    class of resulting entity
     * @param <T>   type of entity
     * @return entity as specified type
     */
    <T> T get(int index, Class<T> cl);

    /**
     * Returns the entity as a specified type
     * This method returns <tt>null</tt> if anything goes wrong.
     *
     * @param index entity index in this row
     * @param cl    class of resulting entity
     * @param <T>   type of entity
     * @return entity as specified type
     */
    <T> T getOrNull(int index, Class<T> cl);

    /**
     * Returns the entity at a specified index
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index in this row
     * @return entity at specified index
     */
    V get(int index);

    /**
     * Returns an entity from the column specified by its head name.
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name of the column
     * @return entity at specified header name column
     */
    V get(H headerName);

    /**
     * Returns entity at an index as {@link String}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return string entity
     * @see #get(int)
     */
    String getString(int index);

    /**
     * Returns entity from header name column as {@link String}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link String} entity
     */
    String getString(H headerName);

    /**
     * Returns entity at an index as {@link Double}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Double} entity
     * @see #get(int)
     */
    Double getDouble(int index);

    /**
     * Returns entity from header name column as {@link Double}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link Double} entity
     */
    Double getDouble(H headerName);

    /**
     * Returns entity at an index as {@link Boolean}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Boolean} entity
     * @see #get(int)
     */
    Boolean getBoolean(int index);

    /**
     * Returns entity from header name column as {@link Boolean}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link Boolean} entity
     */
    Boolean getBoolean(H headerName);

    /**
     * Returns entity at an index as {@link Integer}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Integer} entity
     * @see #get(int)
     */
    Integer getInteger(int index);

    Integer getInteger(H headerName);

    /**
     * Returns entity at an index as {@link Float}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Float} entity
     * @see #get(int)
     */
    Float getFloat(int index);

    /**
     * Returns entity from header name column as {@link Float}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link Float} entity
     */
    Float getFloat(H headerName);


    /**
     * Returns entity at an index as {@link Long}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Long} entity
     * @see #get(int)
     */
    Long getLong(int index);

    /**
     * Returns entity from header name column as {@link Long}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link Long} entity
     */
    Long getLong(H headerName);

    /**
     * Returns entity at an index as {@link Short}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Short} entity
     * @see #get(int)
     */
    Short getShort(int index);

    /**
     * Returns entity from header name column as {@link Short}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link Short} entity
     */
    Short getShort(H headerName);

    /**
     * Returns entity at an index as {@link Byte}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Byte} entity
     * @see #get(int)
     */
    Byte getByte(int index);

    /**
     * Returns entity from header name column as {@link Byte}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name
     * @return {@link Byte} entity
     */
    Byte getByte(H headerName);

    /**
     * Returns the number of entities in this row
     *
     * @return number of entities
     */
    int size();


}
