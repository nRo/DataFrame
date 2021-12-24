/*
 *
 *  * Copyright (c) 2019 Alexander Grün
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

package de.unknownreality.dataframe.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 15.03.2016.
 */
public class StringSplitter {
    private static final char UTF8_BOM = 65279;
    public static StringSplitter create(){
        return new StringSplitter();
    }
    private boolean detectSingleQuotes = true;
    private boolean detectQuotes = true;
    public StringSplitter() {
    }

    public StringSplitter setDetectQuotes(boolean detectQuotes) {
        this.detectQuotes = detectQuotes;
        return this;
    }

    public StringSplitter setDetectSingleQuotes(boolean detectSingleQuotes) {
        this.detectSingleQuotes = detectSingleQuotes;
        return this;
    }

    public boolean isDetectQuotes() {
        return detectQuotes;
    }

    public boolean isDetectSingleQuotes() {
        return detectSingleQuotes;
    }


    /**
     * Split an input string at a specified split-character  into several parts.
     * <tt>"</tt> and <tt>'</tt> are considered during the process.
     * <p><code>"testA    testB   testB" -&gt; [testA,testB,testC]</code></p>
     * <p><code>"'testA    testB'   testB" -&gt; [testA    testB,testC]</code></p>
     *
     * @param input input string
     * @param split char used to split
     * @return string array containing all splitted parts
     */
    public String[] splitQuoted(String input, Character split) {
        List<String> parts = new ArrayList<>();
        splitQuoted(input, split, new ListParts(parts));
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
     * @param parts string array that is filled with the resulting parts
     */
    public void splitQuoted(String input, Character split, String[] parts) {
        splitQuoted(input, split, new ArrayParts(parts));
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
    public void splitQuoted(String input, Character split, Parts parts) {
        if (input.length() == 0) {
            return;
        }
        boolean inQuotation = false;
        boolean inDoubleQuotation = false;
        boolean escapeNext = false;
        char c;
        boolean startOrSplit = true;
        final StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            c = input.charAt(i);
            if ((i == 0 && c == UTF8_BOM) || isInvisible(c)) {
                continue;
            } else if (escapeNext) {
                sb.append(c);
                escapeNext = false;
                continue;
            } else if (c == '\\') {
                escapeNext = true;
                continue;
            } else if (detectSingleQuotes && c == '\'') {
                if (inQuotation) {
                    inQuotation = false;
                } else if (!inDoubleQuotation && startOrSplit) {
                    inQuotation = true;
                    startOrSplit = false;
                } else {
                    sb.append(c);
                }
                continue;
            } else if (detectQuotes && c == '\"') {
                if (inDoubleQuotation) {
                    inDoubleQuotation = false;
                } else if (!inDoubleQuotation && startOrSplit) {
                    inDoubleQuotation = true;
                    startOrSplit = false;
                } else {
                    sb.append(c);
                }
                continue;
            } else if (c == split && !inDoubleQuotation && !inQuotation) {

                parts.add(sb.toString());
                sb.setLength(0);
                startOrSplit = true;
                continue;
            } else {
                startOrSplit = false;
            }
            sb.append(c);

        }
        parts.add(sb.toString());

    }

    private boolean isInvisible(char c) {
        return Character.getType(c) == Character.CONTROL && c != '\t' && c != '\n' && c != '\r';
    }

    private interface Parts {
        void add(String part);
    }

    private static class ListParts implements Parts {
        private final List<String> list;

        public ListParts(List<String> list) {
            this.list = list;
        }

        @Override
        public void add(String part) {
            list.add(part);
        }
    }

    private static class ArrayParts implements Parts {
        private final String[] array;
        private int p = 0;

        public ArrayParts(String[] array) {
            this.array = array;
        }

        @Override
        public void add(String part) {
            array[p++] = part;
        }
    }
}
