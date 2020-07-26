package de.unknownreality.dataframe.type;

import de.unknownreality.dataframe.DataFrameRuntimeException;

public class ValueTypeRuntimeException extends DataFrameRuntimeException {
    public ValueTypeRuntimeException(String message) {
        super(message);
    }

    public ValueTypeRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
