package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class DoubleColumn extends NumberColumn<Double> {
    private static Logger log = LoggerFactory.getLogger(DoubleColumn.class);

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

    private Parser parser = ParserUtil.findParserOrNull(Double.class);
    @Override
    public Parser<Double> getParser() {
        return parser;
    }

    @Override
    public Double median() {
        DoubleColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }


    @Override
    public DoubleColumn add(NumberColumn column) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) + column.get(i).doubleValue());
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
    public DoubleColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) - column.get(i).doubleValue());
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
    public DoubleColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) * column.get(i).doubleValue());
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
    public DoubleColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) / column.get(i).doubleValue());
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
    public DoubleColumn add(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) + value.doubleValue());
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
    public DoubleColumn subtract(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) - value.doubleValue());
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
    public DoubleColumn multiply(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) * value.doubleValue());
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
    public DoubleColumn divide(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) / value.doubleValue());
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
        Double sum = 0d;
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
    public Double sum() {
        int naCount = 0;
        double sum = 0;
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
    public Double min() {
        Double min = Double.MAX_VALUE;
        int naCount = 0;
        double sum = 0;
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
    public Double max() {
        Double max = Double.MIN_VALUE;
        int naCount = 0;
        double sum = 0;
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
    public DoubleColumn copy() {
        Double[] copyValues = new Double[size()];
        toArray(copyValues);
        return new DoubleColumn(getName(), copyValues);
    }

}
