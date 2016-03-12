package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class IntegerColumn extends BasicColumn<Integer> implements NumberColumn<Integer> {
    private static Logger log = LoggerFactory.getLogger(IntegerColumn.class);

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
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) + column.get(i).intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("add() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) - column.get(i).intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("subtract() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) * column.get(i).intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("multiply() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) / column.get(i).intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("divide() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn add(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) + value.intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("add() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn subtract(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) - value.intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("subtract() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn multiply(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) * value.intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("multiply() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public IntegerColumn divide(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) / value.intValue());
            }
            else{
                naCount++;
            }
        }
        if(naCount > 0){
            log.warn("divide() ignored {} NA",naCount);
        }
        return this;
    }

    @Override
    public Double mean() {
        int naCount = 0;
        Integer sum = 0;
        int count = 0;
        int size = size();
        for(int i = 0; i < size;i++){
            if(isNA(i)){
                naCount++;
                continue;
            }
            count++;
            sum += get(i);
        }
        if(naCount > 0){
            log.warn("mean() ignored {} NA",naCount);
        }
        return sum.doubleValue() / count;
    }

    @Override
    public Integer sum() {
        int naCount = 0;
        int sum = 0;
        int size = size();
        for(int i = 0; i < size;i++){
            if(isNA(i)){
                naCount++;
                continue;
            }
            sum += get(i);
        }
        if(naCount > 0){
            log.warn("sum() ignored {} NA",naCount);
        }
        return sum;
    }

    @Override
    public Integer min() {
        Integer min = Integer.MAX_VALUE;
        int naCount = 0;
        int sum = 0;
        int size = size();
        for(int i = 0; i < size;i++){
            if(isNA(i)){
                naCount++;
                continue;
            }
            min = Math.min(min,get(i));
        }
        if(naCount > 0){
            log.warn("min() ignored {} NA",naCount);
        }
        return min;
    }

    @Override
    public Integer max() {
        Integer max = Integer.MIN_VALUE;
        int naCount = 0;
        int sum = 0;
        int size = size();
        for(int i = 0; i < size;i++){
            if(isNA(i)){
                naCount++;
                continue;
            }
            max = Math.max(max,get(i));
        }
        if(naCount > 0){
            log.warn("max() ignored {} NA",naCount);
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
