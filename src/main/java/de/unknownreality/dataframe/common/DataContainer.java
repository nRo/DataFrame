package de.unknownreality.dataframe.common;

import de.unknownreality.dataframe.common.mapping.DataMapper;

import java.util.List;

/**
 * Created by Alex on 14.03.2016.
 */
public interface DataContainer<H extends Header, R extends Row> extends RowIterator<R> {
    /**
     * Returns the header of this data container
     *
     * @return data container header
     */
    H getHeader();

    /**
     * Maps this data container to a list of entities.
     *
     * @param cl  class of resulting entities
     * @param <T> type of entities
     * @return list of mapped entities
     * @see DataMapper#map(DataContainer, Class)
     */
    <T> List<T> map(Class<T> cl);
}
