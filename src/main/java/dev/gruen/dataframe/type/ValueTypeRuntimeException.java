package dev.gruen.dataframe.type;

import dev.gruen.dataframe.DataFrameRuntimeException;

public class ValueTypeRuntimeException extends DataFrameRuntimeException {
    public ValueTypeRuntimeException(String message) {
        super(message);
    }

    public ValueTypeRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
