package de.unknownreality.dataframe.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 15.03.2016.
 */
public class StringUtil {

    /**
     * Puts a string in quotes.
     * All occurrences of quotes chars in the string are escaped.
     *
     * @param input     string to put in quotes
     * @param quoteChar quote char
     * @return string between quote chars
     */
    public static String putInQuotes(String input, Character quoteChar) {
        return quoteChar + input.replace(quoteChar.toString(), "\\" + quoteChar) + quoteChar;
    }

    /**
     * Splits a string and returns the parts as array.
     *
     * @param input string to split
     * @param split split char
     * @return array of parts
     * @see #splitQuoted(String, Character, List)
     */
    public static String[] splitQuoted(String input, Character split) {
        List<String> parts = new ArrayList<>();
        splitQuoted(input, split, parts);
        String[] result = new String[parts.size()];
        return parts.toArray(result);
    }

    /**
     * Split an input string at a specified split-character  into several parts.
     * <tt>"</tt> and <tt>'</tt> are considered during the process.
     * <p><code>"testA    testB   testB" -> [testA,testB,testC]</code></p>
     * <p><code>"'testA    testB'   testB" -> [testA    testB,testC]</code></p>
     *
     * @param input input string
     * @param split char used to split
     * @param parts list filled with the resulting parts
     */
    @SuppressWarnings("ConstantConditions")
    public static void splitQuoted(String input, Character split, List<String> parts) {
        boolean inQuotation = false;
        boolean inDoubleQuotation = false;
        boolean escapeNext = false;
        int currentStart = 0;
        char[] chars = input.trim().toCharArray();
        char c;
        String p;
        for (int i = 0; i < chars.length; i++) {
            c = chars[i];
            boolean escape = escapeNext;
            escapeNext = false;
            if (!escape && c == '\\') {
                escapeNext = true;
            } else if (c == '\'') {
                if (inQuotation && !escape) {
                    inQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation) {
                    inQuotation = true;
                }
            } else if (c == '\"') {
                if (inDoubleQuotation && !escape) {
                    inDoubleQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation) {
                    inDoubleQuotation = true;
                }
            } else if (c == split && !inDoubleQuotation && !inQuotation) {
                int length = i - currentStart;
                if (length == 0) {
                    p = "";
                } else {
                    p = input.substring(currentStart, currentStart + length);
                }
                parts.add(p);
                currentStart = i + 1;
            }
        }
        if (currentStart < chars.length) {
            p = input.substring(currentStart);
            parts.add(p);
        }

    }
}
