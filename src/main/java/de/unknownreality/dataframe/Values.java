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

package de.unknownreality.dataframe;

/**
 * Created by Alex on 12.03.2016.
 */
public class Values {
    /**
     * Representation for 'not available'.
     * Null values in a row are returned as <tt>NA</tt>
     */
    public static final NA NA = new NA();


    public static class NA implements Comparable {



        public boolean isNA(Object o){
            if(o == null){
                return false;
            }
            if(o == this){
                return true;
            }
            if(o instanceof String){
                return "NA".equals(o.toString());
            }
            return false;
        }
        private NA() {

        }

        @Override
        public int compareTo(Object o) {
            if (o == this) {
                return 0;
            }
            return -1;
        }

        public String toString() {
            return "NA";
        }


        public boolean equals(Object o) {
            return o == this;
        }
    }
}
