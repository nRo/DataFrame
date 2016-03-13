package de.unknownreality.data.frame.column;

import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class FloatColumn extends BasicColumn<Float> implements NumberColumn<Float> {
    private static Logger log = LoggerFactory.getLogger(FloatColumn.class);

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
        return ParserUtil.findParserOrNull(Float.class);
    }

    @Override
    public Float median() {
        FloatColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }


    @Override
    public FloatColumn add(NumberColumn column) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) + column.get(i).floatValue());
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
    public FloatColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) - column.get(i).floatValue());
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
    public FloatColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) * column.get(i).floatValue());
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
    public FloatColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) / column.get(i).floatValue());
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
    public FloatColumn add(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) + value.floatValue());
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
    public FloatColumn subtract(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) - value.floatValue());
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
    public FloatColumn multiply(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) * value.floatValue());
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
    public FloatColumn divide(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) / value.floatValue());
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
        Float sum = 0f;
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
    public Float sum() {
        int naCount = 0;
        float sum = 0;
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
    public Float min() {
        Float min = Float.MAX_VALUE;
        int naCount = 0;
        float sum = 0;
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
    public Float max() {
        Float max = Float.MIN_VALUE;
        int naCount = 0;
        float sum = 0;
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
    public FloatColumn copy() {
        Float[] copyValues = new Float[size()];
        System.arraycopy(getValues(), 0, copyValues, 0, size());
        return new FloatColumn(getName(), copyValues);
    }

}
