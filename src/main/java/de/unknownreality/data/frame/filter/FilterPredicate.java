package de.unknownreality.data.frame.filter;

import de.unknownreality.data.common.Row;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class FilterPredicate {
    public abstract boolean valid(Row row);

    public FilterPredicate and(FilterPredicate p) {
        return FilterPredicate.and(this, p);
    }

    public FilterPredicate or(FilterPredicate p) {
        return FilterPredicate.or(this, p);
    }

    public FilterPredicate xor(FilterPredicate p) {
        return FilterPredicate.xor(this, p);
    }

    public FilterPredicate neg(){
        return FilterPredicate.not(this);
    }


    public static FilterPredicate not(final FilterPredicate filterPredicate) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return !filterPredicate.valid(row);
            }
        };
    }
    public static FilterPredicate ne(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row value) {
                return p1.valid(value) != p2.valid(value);
            }
        };
    }
    public static <T> FilterPredicate eq(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) == p2.valid(row);
            }
        };
    }


    public static FilterPredicate and(final FilterPredicate... predicates) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                for(FilterPredicate predicate : predicates){
                    if(!predicate.valid(row)){
                        return false;
                    }
                }
                return true;
            }
        };
    }

    public static FilterPredicate or(final FilterPredicate... predicates) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                for(FilterPredicate predicate : predicates){
                    if(predicate.valid(row)){
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static FilterPredicate and(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) && p2.valid(row);
            }
        };
    }

    public static FilterPredicate or(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) || p2.valid(row);
            }
        };
    }

    public static FilterPredicate xor(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                boolean p1v = p1.valid(row);
                boolean p2v = p2.valid(row);
                return (p1v && !p2v) || (p2v && !p1v);
            }
        };
    }

    public static FilterPredicate ne(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.NE, value);
    }

    public static FilterPredicate eq(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.EQ, value);
    }

    public static FilterPredicate gt(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.GT, value);
    }

    public static FilterPredicate lt(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.LT, value);

    }

    public static FilterPredicate ge(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.GE, value);

    }

    public static FilterPredicate le(final String name, final Object value) {
        return new ComparePredicate(name, ComparePredicate.Operation.LE, value);
    }

    public static FilterPredicate in(final String name, final Object... values) {
        return in(name, Arrays.asList(values));
    }

    public static FilterPredicate in(final String name, final List<Object> values) {
        return in(name,new HashSet<Object>(values));
    }
    public static FilterPredicate in(final String name, final Set<Object> values) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return values.contains(row.get(name));
            }
        };
    }
    public static FilterPredicate btwn(final String name, Object low, Object high){
        return FilterPredicate.and(FilterPredicate.gt(name,low),FilterPredicate.lt(name,high));
    }

}
