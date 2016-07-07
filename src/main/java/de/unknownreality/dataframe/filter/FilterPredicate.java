package de.unknownreality.dataframe.filter;

import de.unknownreality.dataframe.common.Row;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class FilterPredicate {
    public abstract boolean valid(Row row);

    public abstract String toString();

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
            @Override
            public String toString() {
                return "!("+filterPredicate.toString()+")";
            }
        };
    }
    public static FilterPredicate ne(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row value) {
                return p1.valid(value) != p2.valid(value);
            }
            @Override
            public String toString() {
                return "("+p1.toString()+") != ("+p2.toString()+")";
            }
        };
    }
    public static <T> FilterPredicate eq(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) == p2.valid(row);
            }
            @Override
            public String toString() {
                return "("+p1.toString()+") == ("+p2.toString()+")";
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

            @Override
            public String toString() {
                StringBuilder sb  = new StringBuilder();
                for(int i = 0; i < predicates.length;i++){
                    sb.append("(").append(predicates[i]).append(")");
                    if(i < predicates.length - 1){
                        sb.append(" AND ");
                    }
                }
                return sb.toString();
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

            @Override
            public String toString() {
                StringBuilder sb  = new StringBuilder();
                for(int i = 0; i < predicates.length;i++){
                    sb.append("(").append(predicates[i]).append(")");
                    if(i < predicates.length - 1){
                        sb.append(" OR ");
                    }
                }
                return sb.toString();
            }
        };
    }

    public static FilterPredicate and(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) && p2.valid(row);
            }
            @Override
            public String toString() {
                return "("+p1.toString()+") AND ("+p2.toString()+")";
            }
        };
    }

    public static FilterPredicate or(final FilterPredicate p1, final FilterPredicate p2) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return p1.valid(row) || p2.valid(row);
            }

            @Override
            public String toString() {
                return "("+p1.toString()+") OR ("+p2.toString()+")";
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

            @Override
            public String toString() {
                return "("+p1.toString()+") XOR ("+p2.toString()+")";
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

    public static FilterPredicate in(final String name, final Object[] values) {
        List<String> test = new ArrayList<>();
        Collections.sort(test, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return in(name, Arrays.asList(values));
    }

    public static FilterPredicate in(final String name, final Collection<Object> values) {
        return in(name,new HashSet<>(values));
    }

    public static FilterPredicate in(final String name, final Set<Object> values) {
        return new FilterPredicate() {
            @Override
            public boolean valid(Row row) {
                return values.contains(row.get(name));
            }

            @Override
            public String toString() {
                return name +" in " + values.toString();
            }
        };
    }
    public static FilterPredicate btwn(final String name, Object low, Object high){
        return FilterPredicate.and(FilterPredicate.gt(name,low),FilterPredicate.lt(name,high));
    }
    public static FilterPredicate matches(final String name, Pattern pattern){
        return new MatchPredicate(name,pattern);
    }
    public static FilterPredicate matches(final String name, String patternString){
        return new MatchPredicate(name,patternString);
    }

}
