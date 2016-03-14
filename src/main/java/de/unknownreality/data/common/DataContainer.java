package de.unknownreality.data.common;

/**
 * Created by Alex on 14.03.2016.
 */
public interface DataContainer<H extends Header,R extends Row> extends RowIterator<R>{
    public H getHeader();
}
