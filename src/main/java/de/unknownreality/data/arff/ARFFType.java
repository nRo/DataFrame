package de.unknownreality.data.arff;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.ParserUtil;

/**
 * Created by Alex on 15.03.2016.
 */
public enum ARFFType {
    Nominal(String.class),Numeric(Double.class),Date(String.class),String(String.class);
    private Class<?> javaClass;
    private Parser parser;
    private ARFFType(Class<?> javaClass){
        this.javaClass = javaClass;
        this.parser = ParserUtil.findParserOrNull(javaClass);
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    public Parser getParser() {
        return parser;
    }

    public static ARFFType getFromClass(Class<?> cl){
        if(cl == String.class){
            return String;
        }
        if(cl.isArray()){
            return Nominal;
        }
        if(cl.isAssignableFrom(Number.class)){
            return Numeric;
        }
        return String;
    }

    public static ARFFType fromString(String name){
        for(ARFFType type : ARFFType.values()){
            if(type.name().toLowerCase().equals(name.toLowerCase())){
                return type;
            }
        }
        return String;
    }

}
