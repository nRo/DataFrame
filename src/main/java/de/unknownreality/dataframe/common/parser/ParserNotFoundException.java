package de.unknownreality.dataframe.common.parser;

/**
 * Created by Alex on 04.06.2015.
 */
public class ParserNotFoundException extends Exception {
    public ParserNotFoundException(Class<?>c) {
        super("no parser found for "+c.getName());
    }
}
