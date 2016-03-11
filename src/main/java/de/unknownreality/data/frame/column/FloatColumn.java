package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.Parsers;

/**
 * Created by Alex on 09.03.2016.
 */
public class FloatColumn extends BasicColumn<Float> implements NumberColumn<Float> {


    public FloatColumn() {
        super();
    }

    public FloatColumn(String name) {
        super(name);
    }

    public FloatColumn(String name, Float[] values) {
        super(name, values);
    }



    @Override
    public Class<Float> getType() {
        return Float.class;
    }

    @Override
    public Parser<Float> getParser() {
        return Parsers.findParserOrNull(Float.class);
    }

    @Override
    public Float median() {
        FloatColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }


    @Override
    public FloatColumn add(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'append' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) + column.get(i).floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) - column.get(i).floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) * column.get(i).floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) / column.get(i).floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn add(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn subtract(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) - value.floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn multiply(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) * value.floatValue());
        }
        return this;
    }

    @Override
    public FloatColumn divide(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) / value.floatValue());
        }
        return this;
    }

    @Override
    public Double mean() {
        return sum().doubleValue() / size();
    }

    @Override
    public Float sum() {
        float sum = 0;
        for (float num : getValues()) {
            sum += num;
        }
        return sum;
    }

    @Override
    public Float min() {
        Float min = Float.MAX_VALUE;
        for (Float num : getValues()) {
            min = Math.min(min, num);
        }
        return min;
    }

    @Override
    public Float max() {
        Float max = Float.MIN_VALUE;
        for (Float num : getValues()) {
            max = Math.max(max, num);
        }
        return max;
    }

    @Override
    public FloatColumn copy() {
        Float[] copyValues = new Float[size()];
        System.arraycopy(getValues(), 0, copyValues, 0, getValues().length);
        return new FloatColumn(getName(), copyValues);
    }

}
