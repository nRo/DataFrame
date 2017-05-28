package de.unknownreality.dataframe.filter.compile;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 21.05.2017.
 */
public enum FieldFilterOperation {

    EQ("EQ","eq","=","=="),
    NE("NE","ne","!="),
    LE("LE","le","<="),
    LT("LT","lt","<"),
    GE("GE","ge",">="),
    GT("GT","Gt",">");
    private String[] aliases;

    FieldFilterOperation(String... aliases){
        this.aliases = aliases;
    }

    public String[] getAliases() {
        return aliases;
    }


    private final static Map<String,FieldFilterOperation> ALIASES_MAP = new HashMap<>();
    static{
        for(FieldFilterOperation fieldOperation : values()){
            for(String alias : fieldOperation.aliases){
                ALIASES_MAP.put(alias,fieldOperation);
            }
        }
    }

    public static FieldFilterOperation find(String alias){
        return ALIASES_MAP.get(alias);
    }
}
