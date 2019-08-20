/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

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
    GT("GT","Gt",">"),
    LIKE("LIKE");
    private String[] aliases;

    FieldFilterOperation(String... aliases){
        this.aliases = aliases;
    }

    /**
     * Getter for property 'aliases'.
     *
     * @return Value for property 'aliases'.
     */
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
