package de.unknownreality.dataframe.filter.compile;

import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.generated.PredicateLexer;
import de.unknownreality.dataframe.generated.PredicateParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;

/**
 * Created by Alex on 18.05.2017.
 */
public class PredicateCompiler {

    public static FilterPredicate compile(String predicateString){
        predicateString = predicateString.trim();
        if(predicateString.isEmpty()){
            return FilterPredicate.empty();
        }
        ErrorListener errorListener = new ErrorListener(predicateString);

        CharStream stream = CharStreams.fromString(predicateString);
        PredicateLexer lexer = new PredicateLexer(stream);
        lexer.removeErrorListener(ConsoleErrorListener.INSTANCE);
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PredicateParser parser = new PredicateParser(tokens);
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);
        parser.addErrorListener(errorListener);
        FilterPredicateVisitor filterPredicateVisitor = new FilterPredicateVisitor();
        return  filterPredicateVisitor.visit(parser.compilationUnit());
    }

}
