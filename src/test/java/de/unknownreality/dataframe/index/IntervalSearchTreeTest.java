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

package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.index.interval.Interval;
import de.unknownreality.dataframe.index.interval.IntervalSearchTree;
import org.junit.Test;

import java.util.*;

import static de.unknownreality.dataframe.common.NumberUtil.ge;
import static de.unknownreality.dataframe.common.NumberUtil.le;
import static org.junit.Assert.assertEquals;


/**
 * Created by Alex on 28.10.2015.
 */
public class IntervalSearchTreeTest {
    private static final Random RANDOM = new Random(952489);
    @Test
    public void intervalSearchTreeTest(){
        List<Interval> intervalList = new ArrayList<Interval>();
        IntervalSearchTree<Integer> intervalSearchTree = new IntervalSearchTree<Integer>();
        int c = 1000;
        int lb = (int)((double)c * 0.9);
        int hb = (int)((double)c * 0.1);

        int t = (int)((double)c * 0.1);
        for(int i = 0; i < c; i++){
            int low = RANDOM.nextInt(lb);
            int high = low + RANDOM.nextInt(c - low);
            Interval interval = new Interval(low,high);
            intervalList.add(interval);
            intervalSearchTree.add(interval,i);
        }

        List<Interval>searchList = new ArrayList<Interval>();
        for(int i = 0; i < t; i++){
            int low = RANDOM.nextInt(lb);
            int high = low + RANDOM.nextInt(hb);
            Interval interval = new Interval(low,high);
            searchList.add(interval);
        }

        for(Interval searchInterval : searchList){
            Set<Integer> result = new HashSet<Integer>(intervalSearchTree.searchAll(searchInterval));
            Set<Integer> findNaive = new HashSet<Integer>(findNaive(intervalList,searchInterval));
            assertEquals(result,findNaive);
        }
    }

    private List<Integer> findNaive(List<Interval> intervals,Interval searchInterval){
        List<Integer> result = new ArrayList<Integer>();
        for(int i = 0; i < intervals.size(); i++){
            Interval interval = intervals.get(i);
            if(le(interval.getLow(), searchInterval.getHigh()) && ge(interval.getHigh(), searchInterval.getLow())){
                result.add(i);
            }
        }
        return result;
    }

    @Test
    public void intervalSearchTreeStabTest(){
        List<Interval> intervalList = new ArrayList<Interval>();
        IntervalSearchTree<Integer> intervalSearchTree = new IntervalSearchTree<Integer>();
        int c = 1000;
        int lb = (int)((double)c * 0.9);
        int t = (int)((double)c * 0.1);

        for(int i = 0; i < c; i++){
            int low = RANDOM.nextInt(lb);
            int high = low + RANDOM.nextInt(c - low);
            Interval interval = new Interval(low,high);
            intervalList.add(interval);
            intervalSearchTree.add(interval,i);
        }

        List<Integer>searchList = new ArrayList<Integer>();
        for(int i = 0; i < t; i++){
            int value = RANDOM.nextInt(lb);
            searchList.add(value);
        }

        for(Integer value : searchList){
            Set<Integer> result = new HashSet<Integer>(intervalSearchTree.stab(value));
            Set<Integer> naive = new HashSet<Integer>(findStabNaive(intervalList,value));
            assertEquals(result,naive);
        }
    }

    private List<Integer> findStabNaive(List<Interval> intervals,int searchInterval){
        List<Integer> result = new ArrayList<Integer>();
        for(int i = 0; i < intervals.size(); i++){
            Interval interval = intervals.get(i);
            if(ge(searchInterval,  interval.low )&& le(searchInterval ,interval.high) ){
                result.add(i);
            }
        }
        return result;
    }
}
