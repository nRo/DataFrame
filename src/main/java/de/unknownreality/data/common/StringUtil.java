package de.unknownreality.data.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 15.03.2016.
 */
public class StringUtil {

    public static String putInQuotes(String input, Character quoteChar){
        return quoteChar+input.replace(quoteChar.toString(),"\\"+quoteChar)+quoteChar;
    }

    public static String[] splitQuoted(String input, Character split){
        List<String> parts = new ArrayList<>();
        splitQuoted(input,split, parts);
        String[] result = new String[parts.size()];
        return parts.toArray(result);
    }
    public static void splitQuoted(String input,Character split, List<String> parts) {
        boolean inQuotation = false;
        boolean inDoubleQuotation = false;
        boolean escapeNext = false;
        int currentStart = 0;
        char[] chars = input.trim().toCharArray();
        char c;
        String p;
        for (int i = 0; i < chars.length;i++) {
            c = chars[i];
            boolean escape = escapeNext;
            escapeNext = false;
            if (!escape && c == '\\') {
                escapeNext = true;
                continue;
            }
            else if (c == '\'') {
                if (inQuotation && !escape) {
                    inQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation) {
                    inQuotation = true;
                    continue;
                }
            }
            else if (c == '\"') {
                if (inDoubleQuotation && !escape) {
                    inDoubleQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation) {
                    inDoubleQuotation = true;
                    continue;
                }
            }
            else if (c == split && !inDoubleQuotation && !inQuotation) {
                int length = i - currentStart;
                if(length == 0){
                    p = new String();
                }
                else{
                    //p = new String(chars,currentStart,length).trim();
                    p = input.substring(currentStart,currentStart+length);
                }
                parts.add(p);
                currentStart = i + 1;
                continue;
            }
        }
        if (currentStart < chars.length) {
            //p = new String(chars,currentStart,chars.length - currentStart).trim();
            p = input.substring(currentStart);
            parts.add(p);
        }

    }
}
