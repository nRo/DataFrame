package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.Parsers;

/**
 * Created by Alex on 09.03.2016.
 */
public class LongColumn extends BasicColumn<Long> implements NumberColumn<Long> {


    public LongColumn() {
        super();
    }

    public LongColumn(String name) {
        super(name);
    }

    public LongColumn(String name, Long[] values) {
        super(name, values);
    }



    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    @Override
    public Parser<Long> getParser() {
        return Parsers.findParserOrNull(Long.class);
    }

    @Override
    public Long median() {
        LongColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }


    @Override
    public LongColumn add(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'append' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) + column.get(i).longValue());
        }
        return this;
    }

    @Override
    public LongColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) - column.get(i).longValue());
        }
        return this;
    }

    @Override
    public LongColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) * column.get(i).longValue());
        }
        return this;
    }

    @Override
    public LongColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int size = size();
        for (int i = 0; i < size; i++) {
            set(i,get(i) / column.get(i).longValue());
        }
        return this;
    }

    @Override
    public LongColumn add(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.longValue());
        }
        return this;
    }

    @Override
    public LongColumn subtract(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.longValue());
        }
        return this;
    }

    @Override
    public LongColumn multiply(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.longValue());
        }
        return this;
    }

    @Override
    public LongColumn divide(Number value) {
        for (int i = 0; i < size(); i++) {
            set(i,get(i) + value.longValue());
        }
        return this;
    }

    @Override
    public Double mean() {
        return sum().doubleValue() / size();
    }

    @Override
    public Long sum() {
        long sum = 0;
        for (long num : getValues()) {
            sum += num;
        }
        return sum;
    }

    @Override
    public Long min() {
        Long min = Long.MAX_VALUE;
        for (Long num : getValues()) {
            min = Math.min(min, num);
        }
        return min;
    }

    @Override
    public Long max() {
        Long max = Long.MIN_VALUE;
        for (Long num : getValues()) {
            max = Math.max(max, num);
        }
        return max;
    }

    @Override
    public LongColumn copy() {
        Long[] copyValues = new Long[size()];
        System.arraycopy(getValues(), 0, copyValues, 0, getValues().length);
        return new LongColumn(getName(), copyValues);
    }

}
