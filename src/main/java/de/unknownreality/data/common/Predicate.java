package de.unknownreality.data.common;

/**
 * Created by Alex on 11.03.2016.
 */
public abstract class Predicate<T> {
    public abstract boolean valid(T row);

    public Predicate<T> and(Predicate<T> p) {
        return Predicate.and(this, p);
    }

    public Predicate<T> or(Predicate<T> p) {
        return Predicate.or(this, p);
    }

    public Predicate<T> xor(Predicate<T> p) {
        return Predicate.xor(this, p);
    }

    public Predicate<T> neg(){
        return Predicate.not(this);
    }


    public static <T> Predicate<T> not(final Predicate<T> filterPredicate) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                return !filterPredicate.valid(value);
            }
        };
    }
    public static <T> Predicate<T> ne(final Predicate<T> p1, final Predicate<T> p2) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                return p1.valid(value) != p2.valid(value);
            }
        };
    }
    public static <T> Predicate<T> eq(final Predicate<T> p1, final Predicate<T> p2) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                return p1.valid(value) == p2.valid(value);
            }
        };
    }


    public static <T> Predicate<T> and(final Predicate... predicates) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T row) {
                for(Predicate predicate : predicates){
                    if(!predicate.valid(row)){
                        return false;
                    }
                }
                return true;
            }
        };
    }

    public static <T> Predicate<T> or(final Predicate... predicates) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                for(Predicate<T> predicate : predicates){
                    if(predicate.valid(value)){
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static <T> Predicate<T> and(final Predicate<T> p1, final Predicate<T> p2) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                return p1.valid(value) && p2.valid(value);
            }
        };
    }

    public static <T> Predicate<T> or(final Predicate<T> p1, final Predicate<T> p2) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                return p1.valid(value) || p2.valid(value);
            }
        };
    }

    public static <T> Predicate<T> xor(final Predicate<T> p1, final Predicate<T> p2) {
        return new Predicate<T>() {
            @Override
            public boolean valid(T value) {
                boolean p1v = p1.valid(value);
                boolean p2v = p2.valid(value);
                return (p1v && !p2v) || (p2v && !p1v);
            }
        };
    }
}
