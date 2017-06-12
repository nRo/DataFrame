package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.header.BasicTypeHeader;

/**
 * Created by algru on 11.06.2017.
 */
public abstract class DataFrameHeader<H extends DataFrameHeader> extends BasicTypeHeader<String> {

    /**
     * Adds a new data frame column to this header
     * @param column new data frame column
     * @return <tt>self</tt> for method chaining
     */
    public abstract H add(DataFrameColumn<?, ?> column);

    @Override
    public abstract H copy();
}
