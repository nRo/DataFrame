package de.unknownreality.dataframe.common.row;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.Header;

/**
 * Created by Alex on 19.05.2017.
 */
public abstract class UpdateableRow<T,H extends Header<T>,V> extends BasicRow<T,H,V> {
    public UpdateableRow(H header, V[] values, int index) {
        super(header, values, index);
    }
    /**
     * Sets a new value.
     * Throws a {@link DataFrameRuntimeException} if the provided column is not found
     * or if the value is not compatible with the respective column
     *
     * @param headerName column header name
     * @param value      new value
     */
    public void set(T headerName, V value) {
        if (!getHeader().contains(headerName)) {
            throw new DataFrameRuntimeException(String.format("header name not found '%s'", headerName));
        }
        if (!isCompatible(value,headerName)) {
            throw new DataFrameRuntimeException(String.format("the value (%s) is not compatible with this column (%s)", value.getClass(), headerName));
        }
        getValues()[getHeader().getIndex(headerName)] = value;
    }

    public abstract boolean isCompatible(V value, T headerName);

    public abstract boolean isCompatible(V value, int headerIndex);


    /**
     * Sets a new value.
     * Throws a {@link DataFrameRuntimeException} if the provided column is not found
     * or if the value is not compatible with the respective column
     *
     * @param index column index
     * @param value new value
     */
    public void set(int index, V value) {
        if (getHeader().size() <= index || index < 0) {
            throw new DataFrameRuntimeException(String.format("invalid column index '%d'", index));
        }
        if (!isCompatible(value,index)) {
            throw new DataFrameRuntimeException(String.format("the value (%s) is not compatible with this column (%s)", value.getClass(), index));
        }
        getValues()[index] = value;
    }
}
