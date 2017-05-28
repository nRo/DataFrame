/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 15.03.2016.
 */
public class StringUtil {

    private StringUtil(){}
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
     * <p><code>"testA    testB   testB" -&gt; [testA,testB,testC]</code></p>
     * <p><code>"'testA    testB'   testB" -&gt; [testA    testB,testC]</code></p>
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
        boolean startOrSplit = true;
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
                if (!inDoubleQuotation && startOrSplit) {
                    inQuotation = true;
                }
                startOrSplit = false;
            } else if (c == '\"') {
                if (inDoubleQuotation && !escape) {
                    inDoubleQuotation = false;
                    continue;
                }
                if (!inDoubleQuotation && startOrSplit) {
                    inDoubleQuotation = true;
                }
                startOrSplit = false;
            } else if (c == split && !inDoubleQuotation && !inQuotation) {
                int length = i - currentStart;
                if (length == 0) {
                    p = "";
                } else {
                    p = input.substring(currentStart, currentStart + length);
                }
                parts.add(p);
                currentStart = i + 1;
                startOrSplit = true;
            }
            else{
                startOrSplit = false;
            }
        }
        if (currentStart < chars.length) {
            p = input.substring(currentStart);
            parts.add(p);
        }

    }
}
