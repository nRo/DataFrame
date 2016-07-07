package de.unknownreality.dataframe.csv;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVException extends Throwable {
    public CSVException(String message) {
        super(message);
    }

    public CSVException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
