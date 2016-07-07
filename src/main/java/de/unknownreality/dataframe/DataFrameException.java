package de.unknownreality.dataframe;

/**
 * Created by Alex on 07.07.2016.
 */
public class DataFrameException extends Exception {
    public DataFrameException(String message) {
        super(message);
    }

    public DataFrameException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
