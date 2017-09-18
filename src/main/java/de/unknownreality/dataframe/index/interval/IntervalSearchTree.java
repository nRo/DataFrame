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

package de.unknownreality.dataframe.index.interval;


import java.util.*;

import static de.unknownreality.dataframe.common.NumberUtil.*;

public class IntervalSearchTree<T> {

    private IntervalNode<T> root;


    public long getSize() {
        return root.getSubtreeSize();
    }

    public int getHeight() {
        return root.getSubtreeHeight();
    }


    public boolean contains(Interval interval) {
        return find(interval) != null;
    }

    public T find(Interval interval) {
        return find(root, interval);
    }

    private T find(IntervalNode<T> node, Interval interval) {
        if (node == null) {
            return null;
        }
        int c = interval.compareTo(node.getInterval());
        if (c < 0) {
            return find(node.getLeft(), interval);
        }
        if (c > 0) {
            return find(node.getRight(), interval);
        }
        return node.getValue();
    }


    public void add(Interval interval, T value) {
        root = recInsert(root, interval, value);
    }


    private Random random = new Random();

    private IntervalNode<T> recInsert(IntervalNode<T> node, Interval interval, T value) {
        if (node == null) {
            return new IntervalNode<>(interval, value);
        }
        if (random.nextDouble() * node.getSubtreeSize() <= 1.0) {
            return rootInsert(node, interval, value);
        }
        int c = interval.compareTo(node.getInterval());
        if (c < 0) {
            node.setLeft(recInsert(node.getLeft(), interval, value));
        } else {
            node.setRight(recInsert(node.getRight(), interval, value));
        }
        updateNode(node);
        return node;
    }

    private IntervalNode<T> rootInsert(IntervalNode<T> node, Interval interval, T value) {
        if (node == null) {
            return new IntervalNode<>(interval, value);
        }
        int c = interval.compareTo(node.getInterval());
        if (c < 0) {
            node.setLeft(recInsert(node.getLeft(), interval, value));
            node = rotateRight(node);
        } else {
            node.setRight(recInsert(node.getRight(), interval, value));
            node = rotateLeft(node);
        }
        return node;
    }

    private IntervalNode<T> rotateRight(IntervalNode<T> node) {
        IntervalNode<T> tmp = node.getLeft();
        node.setLeft(tmp.getRight());
        tmp.setRight(node);
        updateNode(node);
        updateNode(tmp);
        return tmp;
    }

    private IntervalNode<T> rotateLeft(IntervalNode<T> node) {
        IntervalNode<T> tmp = node.getRight();
        node.setRight(tmp.getLeft());
        tmp.setLeft(node);
        updateNode(node);
        updateNode(tmp);
        return tmp;
    }

    private void updateNode(IntervalNode node) {
        if (node == null) {
            return;
        }
        updateMax(node);
        updateSubtreeSize(node);
    }

    private void updateSubtreeSize(IntervalNode node) {
        long size = 1;
        if (node.getLeft() != null) {
            size += node.getLeft().getSubtreeSize();
        }
        if (node.getRight() != null) {
            size += node.getRight().getSubtreeSize();
        }
        node.setSubtreeSize(size);
    }

    private void updateMax(IntervalNode node) {
        Number max = node.getInterval().high;
        Number maxL = node.getLeft() != null ? node.getLeft().getMax() : Double.NEGATIVE_INFINITY;
        Number maxR = node.getRight() != null ? node.getRight().getMax() : Double.NEGATIVE_INFINITY;
        max = max(max, max(maxL, maxR));
        node.setMax(max);
    }


    public void remove(Interval interval) {
        root = remove(root, interval);
    }

    private IntervalNode<T> remove(IntervalNode<T> node, Interval interval) {
        if (node == null) return null;
        int c = interval.compareTo(node.getInterval());
        if(c < 0){
            node.setLeft(remove(node.getLeft(),interval));
        }
        else if (c > 0){
            node.setRight(remove(node.getRight(), interval));
        }
        else{
            node = joinLeftRight(node.getLeft(), node.getRight());
        }
        updateNode(node);
        return node;
    }

    private IntervalNode<T> joinLeftRight(IntervalNode<T> a, IntervalNode<T> b) {
        if (a == null) return b;
        if (b == null) return a;

        if (random.nextDouble() * (a.getSubtreeSize() + b.getSubtreeSize()) < a.getSubtreeSize())  {
            a.setRight(joinLeftRight(a.getRight(), b));
            updateNode(a);
            return a;
        }
        else {
            a.setRight(joinLeftRight(a, b.getLeft()));
            updateNode(b);
            return b;
        }
    }

    public List<T> searchAll(Interval interval) {
        List<T> result = new ArrayList<>();
        recSearchAll(root, interval, result);
        return result;
    }

    public List<T> searchAll(Number low, Number high) {
        List<T> result = new ArrayList<>();
        recSearchAll(root, new Interval(low, high), result);
        return result;
    }

    private boolean recSearchAll(IntervalNode<T> node, Interval interval, List<T> result) {
        if (node == null) {
            return false;
        }
        boolean found = false;
        if (interval.intersects(node.getInterval())) {
            result.add(node.getValue());
            found = true;
        }
        boolean foundLeft = false;
        if (node.getLeft() != null && le(interval.low, node.getLeft().getMax())) {
            foundLeft = recSearchAll(node.getLeft(), interval, result);
        }
        if (foundLeft || node.getLeft() == null || gt(interval.low, node.getLeft().getMax())) {
            found = found | recSearchAll(node.getRight(), interval, result);
        }
        return found || foundLeft;
    }


    public List<T> stab(Number value) {
        List<T> result = new ArrayList<>();
        recStab(root, value, result);
        return result;
    }


    private boolean recStab(IntervalNode<T> node, Number value, List<T> result) {
        if (node == null) {
            return false;
        }
        if (gt(value, node.getMax())) {
            return false;
        }
        boolean found = false;
        if (node.getInterval().contains(value)) {
            result.add(node.getValue());
            found = true;
        }
        boolean foundLeft = false;
        if (node.getLeft() != null && le(value, node.getLeft().getMax())) {
            foundLeft = recStab(node.getLeft(), value, result);
        }
        if (foundLeft || node.getLeft() == null || gt(value, node.getLeft().getMax())) {
            found = found | recStab(node.getRight(), value, result);
        }
        return found || foundLeft;
    }

    public void clear(){
        this.root = null;
    }
}
