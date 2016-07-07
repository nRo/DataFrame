package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class LongColumn extends  NumberColumn<Long> {
    private static Logger log = LoggerFactory.getLogger(LongColumn.class);

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

    private Parser parser = ParserUtil.findParserOrNull(Long.class);
    @Override
    public Parser<Long> getParser() {
        return parser;
    }

    @Override
    public Long median() {
        LongColumn sorted = copy();
        sorted.sort();
        return sorted.get(size() / 2);
    }


    @Override
    public LongColumn add(NumberColumn column) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) + column.get(i).longValue());
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
    public LongColumn subtract(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'subtract' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) - column.get(i).longValue());
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
    public LongColumn multiply(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'multiply' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) * column.get(i).longValue());
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
    public LongColumn divide(NumberColumn column) {
        if (column.size() != size()) {
            throw new IllegalArgumentException("'divide' required column of same size");
        }
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && ! column.isNA(i)){
                set(i,get(i) / column.get(i).longValue());
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
    public LongColumn add(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) + value.longValue());
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
    public LongColumn subtract(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) - value.longValue());
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
    public LongColumn multiply(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) * value.longValue());
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
    public LongColumn divide(Number value) {
        int naCount = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            if(!isNA(i) && value != null){
                set(i,get(i) / value.longValue());
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
        Long sum = 0l;
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
    public Long sum() {
        int naCount = 0;
        long sum = 0;
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
    public Long min() {
        Long min = Long.MAX_VALUE;
        int naCount = 0;
        long sum = 0;
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
    public Long max() {
        Long max = Long.MIN_VALUE;
        int naCount = 0;
        long sum = 0;
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
    public LongColumn copy() {
        Long[] copyValues = new Long[size()];
        toArray(copyValues);
        return new LongColumn(getName(), copyValues);
    }

}
