package de.unknownreality.data.arff;

/**
 * Created by Alex on 09.03.2016.
 */
public class ARFFException extends Throwable{
    public ARFFException(String msg){
        super(String.format("arff error: %s",msg));
    }
}
