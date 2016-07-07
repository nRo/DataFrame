package de.unknownreality.dataframe.common;

/**
 * Created by Alex on 10.03.2016.
 */
public interface Row<V> {

    /**
     * Returns an entity using head name as a specified type.
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param headerName header name of column
     * @param cl         class of resulting entity
     * @param <T>        type of entity
     * @return entity as specified type
     */
    <T> T get(String headerName, Class<T> cl);


    /**
     * Returns an entity using head name as a specified type.
     * This method returns <tt>null</tt> if anything goes wrong.
     *
     * @param headerName header name of column
     * @param cl         class of resulting entity
     * @param <T>        type of entity
     * @return entity as specified type
     */
    <T> T getOrNull(String headerName, Class<T> cl);


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
    V get(String headerName);

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
    String getString(String headerName);

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
    Double getDouble(String headerName);

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
    Boolean getBoolean(String headerName);

    /**
     * Returns entity at an index as {@link Integer}
     * This method throws a {@link RuntimeException} if anything goes wrong.
     *
     * @param index entity index
     * @return {@link Integer} entity
     * @see #get(int)
     */
    Integer getInteger(int index);

    Integer getInteger(String headerName);

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
    Float getFloat(String headerName);


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
    Long getLong(String headerName);

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
    Short getShort(String headerName);

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
    Byte getByte(String headerName);

    /**
     * Returns the number of entities in this row
     *
     * @return number of entities
     */
    int size();


}
