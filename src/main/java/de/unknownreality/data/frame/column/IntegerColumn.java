package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.Parsers;

/**
 * Created by Alex on 09.03.2016.
 */
public class IntegerColumn extends BasicColumn<Integer> implements NumberColumn<Integer> {


    public IntegerColumn() {
        super();
    }

    public IntegerColumn(String name) {
        super(name);
    }

    public IntegerColumn(String name, Integer[] values) {
        super(name, values);
    }



    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }

    @Override
    public Parser<Integer> getParser() {
        return Parsers.findParserOrNull(Integer.class);
    }

    @Override
    public Integer median() {
        IntegerColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }


    @Override
    public IntegerColumn add(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'append' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) + column.get(i).intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) - column.get(i).intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) * column.get(i).intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) / column.get(i).intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn add(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn subtract(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn multiply(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.intValue());
        }
        return this;
    }

    @Override
    public IntegerColumn divide(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.intValue());
        }
        return this;
    }

    @Override
    public Double mean() {
        return sum().doubleValue() / size();
    }

    @Override
    public Integer sum() {
        int sum = 0;
        for (int num : getValues()) {
            sum += num;
        }
        return sum;
    }

    @Override
    public Integer min() {
        Integer min = Integer.MAX_VALUE;
        for (Integer num : getValues()) {
            min = Math.min(min, num);
        }
        return min;
    }

    @Override
    public Integer max() {
        Integer max = Integer.MIN_VALUE;
        for (Integer num : getValues()) {
            max = Math.max(max, num);
        }
        return max;
    }

    @Override
    public IntegerColumn copy() {
        Integer[] copyValues = new Integer[size()];
        System.arraycopy(getValues(), 0, copyValues, 0, getValues().length);
        return new IntegerColumn(getName(), copyValues);
    }

}
