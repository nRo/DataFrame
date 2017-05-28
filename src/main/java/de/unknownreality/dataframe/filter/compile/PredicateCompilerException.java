package de.unknownreality.dataframe.filter.compile;

/**
 * Created by Alex on 21.05.2017.
 */
public class PredicateCompilerException extends RuntimeException {
    public PredicateCompilerException(String message) {
        super(message);
    }

    public PredicateCompilerException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
