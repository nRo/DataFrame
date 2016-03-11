package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.Parsers;

/**
 * Created by Alex on 09.03.2016.
 */
public class DoubleColumn extends BasicColumn<Double> implements NumberColumn<Double> {


    public DoubleColumn() {
        super();
    }

    public DoubleColumn(String name) {
        super(name);
    }

    public DoubleColumn(String name, Double[] values) {
        super(name, values);
    }



    @Override
    public Class<Double> getType() {
        return Double.class;
    }

    @Override
    public Parser<Double> getParser() {
        return Parsers.findParserOrNull(Double.class);
    }

    @Override
    public Double median() {
        DoubleColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }

    @Override
    public Double get(int index) {
        return super.get(index);
    }

    @Override
    public DoubleColumn add(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'append' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) + column.get(i).doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) - column.get(i).doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) * column.get(i).doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) / column.get(i).doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn add(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i)+value.doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn subtract(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i)-value.doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn multiply(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i)*value.doubleValue());
        }
        return this;
    }

    @Override
    public DoubleColumn divide(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i)/value.doubleValue());
        }
        return this;
    }

    @Override
    public Double mean() {
        return sum() / size();
    }

    @Override
    public Double sum() {
        double sum = 0;
        for (double num : getValues()) {
            sum += num;
        }
        return sum;
    }

    @Override
    public Double min() {
        Double min = Double.MAX_VALUE;
        for (double num : getValues()) {
            min = Math.min(min, num);
        }
        return min;
    }

    @Override
    public Double max() {
        Double max = Double.MIN_VALUE;
        for (double num : getValues()) {
            max = Math.max(max, num);
        }
        return max;
    }

    @Override
    public DoubleColumn copy() {
        Double[] copyValues = new Double[size()];
        System.arraycopy(getValues(), 0, copyValues, 0, getValues().length);
        return new DoubleColumn(getName(), copyValues);
    }

}
