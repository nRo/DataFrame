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

    public static String[] splitQuoted(String input,Character split) {
        List<String> parts = new ArrayList<String>();
        boolean inQuotation = false;
        boolean inDoubleQuotation = false;
        StringBuilder currentPart = new StringBuilder();
        boolean escapeNext = false;
        for (char c : input.trim().toCharArray()) {
            boolean escape = escapeNext;
            escapeNext = false;
            if (!escape && c == '\\') {
                escapeNext = true;
                continue;
            }
            if (c == '\'') {
                if (inQuotation && !escape) {
                    inQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation) {
                    inQuotation = true;
                    continue;
                }
            }
            if (c == '\"') {
                if (inDoubleQuotation && !escape) {
                    inDoubleQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation) {
                    inDoubleQuotation = true;
                    continue;
                }
            }
            if (c == split && !inDoubleQuotation && !inQuotation) {
                String p = currentPart.toString().trim();
                currentPart = new StringBuilder();
                if (!p.isEmpty()) {
                    parts.add(p);
                }
                continue;
            }
            currentPart.append(c);
        }
        String p = currentPart.toString().trim();
        if (!p.isEmpty()) {
            parts.add(p);
        }
        String[] result = new String[parts.size()];
        return parts.toArray(result);
    }
}
