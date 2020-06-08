package de.unknownreality.dataframe.filter;

import de.unknownreality.dataframe.DataFrameRuntimeException;

public class DataFrameFilterRuntimeException extends DataFrameRuntimeException {
    public DataFrameFilterRuntimeException(String message) {
        super(message);
    }

    public DataFrameFilterRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
