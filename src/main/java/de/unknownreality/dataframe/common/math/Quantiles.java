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

package de.unknownreality.dataframe.common.math;

import de.unknownreality.dataframe.common.NumberUtil;

import java.util.Arrays;

/**
 * Created by Alex on 17.07.2017.
 */
public class Quantiles<T extends Number> {
    private T[] values;
    private Class<T> cl;
    public Quantiles(T[] values, Class<T> cl, boolean sorted){
        if(values == null || values.length == 0){
            throw new IllegalArgumentException("empty value arrays are not allowed for quantile calculations");
        }
        this.values = values;
        this.cl = cl;
        if(!sorted){
            Arrays.sort(values);
        }
    }

    public T getQuantile(double quantile) {
        int index = (int) Math.ceil(quantile * values.length) - 1;
        index = index < 0 ? 0 : index;
        return values[index];
    }
    public T median() {
        return NumberUtil.convert(values[values.length / 2], cl);
    }

    public T max(){
        return values[values.length -1 ];
    }

    public T min(){
        return values[0];
    }
}
