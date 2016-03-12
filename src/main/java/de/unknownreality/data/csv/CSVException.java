package de.unknownreality.data.csv;

import java.io.File;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVException extends Throwable{
    public CSVException(String msg){
        super(String.format("csv error: {}",msg));
    }
}
