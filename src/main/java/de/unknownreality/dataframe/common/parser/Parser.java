package de.unknownreality.dataframe.common.parser;

import java.text.ParseException;

/**
 * Created by Alex on 04.06.2015.
 */
    public abstract class Parser<T> {

    public abstract T parse(String s)  throws ParseException;

}
