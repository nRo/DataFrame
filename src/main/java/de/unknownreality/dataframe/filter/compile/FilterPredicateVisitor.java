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

import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.generated.PredicateBaseVisitor;
import de.unknownreality.dataframe.generated.PredicateParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 21.05.2017.
 */
public class FilterPredicateVisitor extends PredicateBaseVisitor<FilterPredicate> {
    private static Logger log = LoggerFactory.getLogger(FieldFilterVisitor.class);

    @Override
    public FilterPredicate visitCompilationUnit(PredicateParser.CompilationUnitContext ctx) {
        if(ctx.predicate() == null){
            throw new PredicateCompilerException("no predicate found");
        }
        return visitPredicate(ctx.predicate());
    }

    @Override
    public FilterPredicate visitPredicate(PredicateParser.PredicateContext ctx){
        if(ctx.field_filter() != null){
            FieldFilterVisitor fieldFilterVisitor = new FieldFilterVisitor();
            return fieldFilterVisitor.visitField_filter(ctx.field_filter());
        }

        if(ctx.predicate().size() == 1){
            FilterPredicateVisitor predicateVisitor = new FilterPredicateVisitor();
            FilterPredicate result = predicateVisitor.visit(ctx.predicate(0));
            if(ctx.NEGATE() != null){
                result = result.neg();
            }
            return result;
        }

        FilterPredicateVisitor predicateVisitorA = new FilterPredicateVisitor();
        FilterPredicateVisitor predicateVisitorB = new FilterPredicateVisitor();

        FilterPredicate predicateA = predicateVisitorA.visitPredicate(ctx.predicate(0));
        FilterPredicate predicateB = predicateVisitorB.visitPredicate(ctx.predicate(1));
        FilterPredicate result =  createPredicate(predicateA,predicateB,ctx.PREDICATE_OPERATION().getText());
        if(ctx.NEGATE() != null){
            result = result.neg();
        }
        return result;
    }


    private static FilterPredicate createPredicate(FilterPredicate predicateA, FilterPredicate predicateB, String operation){
        PredicateOperation predicateOperation = PredicateOperation.find(operation);

        switch (predicateOperation){
            case AND: return FilterPredicate.and(predicateA,predicateB);
            case OR: return FilterPredicate.or(predicateA,predicateB);
            case XOR: return FilterPredicate.xor(predicateA,predicateB);
            case NOR: return FilterPredicate.nor(predicateA,predicateB);

            default: throw new PredicateCompilerException(String.format("unsupported predicate operation '%s'",operation));

        }
    }
}
