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
            throw new PredicateCompilerException(String.format("no predicate found"));
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
