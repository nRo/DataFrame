package de.unknownreality.dataframe.csv;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVRuntimeException extends RuntimeException {
    public CSVRuntimeException(String message) {
        super(message);
    }

    public CSVRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
