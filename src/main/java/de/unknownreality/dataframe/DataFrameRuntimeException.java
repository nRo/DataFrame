package de.unknownreality.dataframe;

/**
 * Created by Alex on 07.07.2016.
 */
public class DataFrameRuntimeException extends RuntimeException {
    public DataFrameRuntimeException(String message) {
        super(message);
    }

    public DataFrameRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
