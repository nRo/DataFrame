package de.unknownreality.dataframe.filter.compile;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 21.05.2017.
 */
public enum PredicateOperation {
    AND("AND","and","&","&&"),
    OR("OR","or","|","||"),
    NOR("NOR","nor"),
    XOR("NOR","nor");

    private String[] aliases;

    PredicateOperation(String... aliases){
        this.aliases = aliases;
    }

    public String[] getAliases() {
        return aliases;
    }


    private final static Map<String,PredicateOperation> ALIASES_MAP = new HashMap<>();
    static{
        for(PredicateOperation predicateOperation : values()){
            for(String alias : predicateOperation.aliases){
                ALIASES_MAP.put(alias,predicateOperation);
            }
        }
    }

    public static PredicateOperation find(String alias){
        return ALIASES_MAP.get(alias);
    }
}
