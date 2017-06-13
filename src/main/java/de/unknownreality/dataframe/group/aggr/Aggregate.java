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

package de.unknownreality.dataframe.group.aggr;

import de.unknownreality.dataframe.common.NumberUtil;
import de.unknownreality.dataframe.group.DataGroup;

/**
 * Created by Alex on 13.06.2017.
 */
public class Aggregate {
    public static final AggregateFunction<Integer> count = (group -> group.size());
    public static AggregateFunction<Integer> count(){
        return count;
    }

    public static AggregateFunction<Double> mean(final String colName){
        return group -> group.getNumberColumn(colName).mean();
    }

    public static AggregateFunction<Double> median(final String colName){
        return median(colName,Double.class);
    }

    public static<T extends Number> AggregateFunction<T> median(final String colName, Class<T> type){
        return group -> NumberUtil.convert(
                group.getNumberColumn(colName).median(), type
        );
    }

    public static AggregateFunction<Double> min(final String colName){
        return min(colName,Double.class);
    }

    public static<T extends Number> AggregateFunction<T> min(final String colName, Class<T> type){
        return group -> NumberUtil.convert(
                group.getNumberColumn(colName).min(), type
        );
    }

    public static AggregateFunction<Double> max(final String colName){
        return max(colName,Double.class);
    }

    public static<T extends Number> AggregateFunction<T> max(final String colName, Class<T> type){
        return group -> NumberUtil.convert(
                group.getNumberColumn(colName).max(), type
        );
    }





}
