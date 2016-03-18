package de.unknownreality.data.frame.filter;

import de.unknownreality.data.common.Row;

import java.math.BigDecimal;

/**
 * Created by Alex on 09.03.2016.
 */
public class ComparePredicate extends FilterPredicate{
    enum Operation{
        GT,GE,LT,LE,EQ,NE
    }
    private String headerName;
    private Object value;
    private Operation operation;

    public ComparePredicate(String headerName,Operation op, Object value){
        this.headerName = headerName;
        this.operation = op;
        this.value = value;
    }
    @Override
    public boolean valid(Row row) {
        Object v = row.get(headerName);
        if(operation == Operation.EQ && v.equals(value)){
            return true;
        }
        boolean numberCompare = (v instanceof Number && value instanceof Number);
        if(!v.getClass().equals(value.getClass()) && !numberCompare){
            if(operation == Operation.NE){
                return true;
            }
            return false;
        }
        int c = 0;
        if(numberCompare){
            //could be better to convert to BigDecimal for comparison
            c = Double.compare(Number.class.cast(v).doubleValue(),Number.class.cast(value).doubleValue());
        }
        else if(v instanceof Comparable && value instanceof Comparable){
            c = ((Comparable)v).compareTo(value);
        }
        switch (operation){
            case GT:
                return c > 0;
            case GE:
                return c >= 0;
            case LT:
                return c < 0;
            case LE:
                return c <= 0;
            case EQ:
                return c == 0;
            case NE:
                return c != 0;
        }
        return false;
    }
}
