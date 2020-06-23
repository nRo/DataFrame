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

package de.unknownreality.dataframe.common.mapping;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Alex on 08.03.2016.
 */
public class DataMapper<T> implements Iterable<T> {
    private static final Logger log = LoggerFactory.getLogger(DataMapper.class);
    private final DataContainer<? extends Header<String>, ? extends Row<?, String>> reader;
    private FieldColumn[] columns;
    private final Class<T> cl;

    private DataMapper(DataContainer<? extends Header<String>, ? extends Row<?, String>> reader, Class<T> cl) {
        this.reader = reader;
        this.cl = cl;
        initFields(reader.getHeader());
    }

    /**
     * Maps a {@link DataContainer} to a list
     * The specified type of entities in the list must have {@link MappedColumn} annotated fields.
     *
     * @param reader the data container
     * @param cl     class of mapped entities
     * @param <T>    type if mapped entities
     * @return List of mapped entities
     */
    public static <T> List<T> map(DataContainer<? extends Header<String>, ? extends Row<?, String>> reader, Class<T> cl) {
        DataMapper<T> mapper = new DataMapper<>(reader, cl);

        return mapper.map();
    }

    /**
     * Returns an iterable over mapped entities from the data container
     * The specified type of entities must have {@link MappedColumn} annotated fields.
     *
     * @param reader the data container
     * @param cl     class of mapped entities
     * @param <T>    type if mapped entities
     * @return iterable over mapped entities
     */
    public static <T> Iterable<T> mapEach(DataContainer<? extends Header<String>, ? extends Row<?, String>> reader, Class<T> cl) {
        return new DataMapper<>(reader, cl);
    }


    /**
     * Maps the dataContainer to a list
     *
     * @return List of mapped entities
     */
    public List<T> map() {
        List<T> result = new ArrayList<>();
        initFields(reader.getHeader());
        for (Row<?, String> row : reader) {
            result.add(processRow(row));
        }
        return result;
    }


    /**
     * Maps the fields from a header to {@link FieldColumn FieldColumns}
     *
     * @param header header of the data container
     */
    private void initFields(Header<String> header) {

        List<FieldColumn> fieldColumnList = new ArrayList<>();
        for (Field field : cl.getDeclaredFields()) {
            String name = field.getName();
            MappedColumn annotation = field.getAnnotation(MappedColumn.class);
            if (annotation == null) {
                continue;
            }
            String headerName = annotation.header();
            if (!isValid(headerName, header)
                    && annotation.index() != -1
                    && annotation.index() < header.size()) {
                headerName = header.get(annotation.index());

            }
            if (!isValid(headerName, header)) {
                if (isValid(name, header)) {
                    headerName = name;
                } else {
                    log.error("{} not found in file", annotation.toString());
                    continue;
                }
            }

            fieldColumnList.add(new FieldColumn(field, headerName));
        }
        columns = new FieldColumn[fieldColumnList.size()];
        fieldColumnList.toArray(columns);
    }

    /**
     * Returns true of the header name is not empty and the header contains this name
     *
     * @param headerName column header name
     * @param header     header object
     * @return <tt>true</tt> header contains name
     */
    private <T> boolean isValid(T headerName, Header<T> header) {
        return !"".equals(headerName) && header.contains(headerName);
    }


    /**
     * Reads a row from the data container and maps it to an entity
     *
     * @param row row from the data container
     * @return mapped entity
     */
    private <R extends Row<?, String>> T processRow(R row) {
        T obj = null;
        try {
            obj = cl.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        for (FieldColumn fieldColumn : columns) {
            fieldColumn.set(row, obj);
        }
        return obj;
    }

    /**
     * Returns an iterator that wraps the row iterator from the data container
     *
     * @return iterator over mapped entities
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            final Iterator<? extends Row<?, String>> rowIterator = reader.iterator();

            @Override
            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported by this iterator");
            }

            @Override
            public T next() {
                if(!rowIterator.hasNext()){
                    throw new NoSuchElementException("index out of bounds");
                }
                return processRow(rowIterator.next());
            }
        };
    }
}
