/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.filter.compile;

import de.unknownreality.dataframe.common.parser.ParserUtil;
import de.unknownreality.dataframe.filter.ColumnComparePredicate;
import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.generated.PredicateBaseVisitor;
import de.unknownreality.dataframe.generated.PredicateParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Alex on 21.05.2017.
 */
public class FieldFilterVisitor extends PredicateBaseVisitor<FilterPredicate> {

    @Override
    public FilterPredicate visitField_filter(PredicateParser.Field_filterContext ctx) {
        FilterPredicate result = null;
        if(ctx.regex_filter() != null){
            RegexFilterVisitor regexFilterVisitor = new RegexFilterVisitor();
            result = regexFilterVisitor.visitRegex_filter(ctx.regex_filter());
        }
        else if(ctx.column_predicate() != null){
            ColumnPredicateFilterVisitor columnPredicateFilterVisitor = new ColumnPredicateFilterVisitor();
            result = columnPredicateFilterVisitor.visitColumn_predicate(ctx.column_predicate());
        }
        else if(ctx.boolean_filter() != null){
            BooleanFilterVisitor booleanFilterVisitor = new BooleanFilterVisitor();
            result = booleanFilterVisitor.visitBoolean_filter(ctx.boolean_filter());
        }
        else{
            result = createFieldFilter(ctx);
        }
        if(ctx.NEGATE() != null){
            result = result.neg();
        }
        return result;
    }

    private static FilterPredicate createFieldFilter(PredicateParser.Field_filterContext ctx){
        String colName = getColname(ctx.variable().getText());
        String operation = ctx.FIELD_OPERATION().getText();
        Comparable value = getValue(ctx);

        return createFieldFilter(colName,value,operation);
    }


    private static FilterPredicate createFieldFilter(String colName, Comparable value, String operation){
        FieldFilterOperation fieldFilterOperation = FieldFilterOperation.find(operation);

        switch (fieldFilterOperation){
            case EQ: return FilterPredicate.eq(colName,value);
            case NE: return FilterPredicate.ne(colName,value);
            case LE: return FilterPredicate.le(colName,value);
            case LT: return FilterPredicate.lt(colName,value);
            case GE: return FilterPredicate.ge(colName,value);
            case GT: return FilterPredicate.gt(colName,value);

            default: throw new PredicateCompilerException(String.format("unsupported filter operation '%s'",operation));

        }
    }
    public static String getColname(String text){
        if(text.startsWith(".")){
            text = text.substring(1);
        }
        String colName = text;
        if(colName.startsWith("'")){
            return colName.substring(1,colName.length() - 1);
        }
        return colName;
    }

    private static Pattern NUMBER_PATTERN = Pattern.compile("[0-9]+([\\.,][0-9]+)?");
    private static Comparable getValue(PredicateParser.Field_filterContext ctx){
        if(ctx.value().NUMBER() != null){
            String n  =ctx.value().NUMBER().getText();
            try {
                if (n.contains(".")) {
                    return ParserUtil.parse(Double.class,n);
                } else {
                    return ParserUtil.parse(Long.class,n);
                }
            }
            catch (Exception e){
                throw new PredicateCompilerException(String.format("error parsing value '%s'",ctx.value().BOOLEAN_VALUE().getText()));
            }
        }
        if(ctx.value().BOOLEAN_VALUE() != null){
            try{
                return ParserUtil.parse(Boolean.class,ctx.value().BOOLEAN_VALUE().getText());
            }
            catch (Exception e){
                throw new PredicateCompilerException(String.format("error parsing value '%s'",ctx.value().BOOLEAN_VALUE().getText()));
            }
        }
        String value = ctx.value().getText();
        if(value.startsWith("'")){
            return value.substring(1,value.length() - 1);
        }
        Matcher m = NUMBER_PATTERN.matcher(value);
        if(m.matches()){
            String n = m.group();
            n.replace(",","");
            if(n.contains(".")){
                return Double.parseDouble(n);
            }
            else{
                return Long.parseLong(n);
            }
        }
        return value;
    }

}
