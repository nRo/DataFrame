package de.unknownreality.data.common;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by Alex on 10.03.2016.
 */
public class MultiIterator<T> implements Iterator<T[]>, Iterable<T[]> {


    public static <T> MultiIterator<T> create(Iterable<T>[] its,Class<T> cl){
        return new MultiIterator<>(its,cl);
    }

    public static <T> MultiIterator<T> create(Collection<? super Iterable<T>> its, Class<T> cl){
        Iterable<T>[] itsArray = new Iterable[its.size()];
        its.toArray(itsArray);
        return new MultiIterator<>(itsArray,cl);
    }

    @Override
    public void remove() {

    }

    private Iterator<T>[] iterators;
    private T[] next;
    private Class<T> cl;
    public MultiIterator(Iterable<T>[] iterables, Class<T> cl){
        iterators = new Iterator[iterables.length];
        this.cl = cl;
        for(int i = 0; i < iterables.length;i++){
            iterators[i] = iterables[i].iterator();
        }
        next = getNext();
    }





    private T[] getNext(){
        final T[] next = (T[]) Array.newInstance(cl, iterators.length);
        boolean found = false;
        for(int i = 0; i < iterators.length;i++){
            if(iterators[i].hasNext()){
                next[i] = iterators[i].next();
                found = true;
            }
            else{
                next[i] = null;
            }

        }
        if(!found){
            return null;
        }
        return next;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public T[] next() {
        T[] rows = next;
        next = getNext();
        return rows;
    }

    @Override
    public Iterator<T[]> iterator() {
        return this;
    }
}
