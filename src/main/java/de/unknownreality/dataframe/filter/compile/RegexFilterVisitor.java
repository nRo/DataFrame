package de.unknownreality.dataframe.filter.compile;

import de.unknownreality.dataframe.filter.FilterPredicate;
import de.unknownreality.dataframe.generated.PredicateBaseVisitor;
import de.unknownreality.dataframe.generated.PredicateParser;

import java.util.regex.Pattern;

/**
 * Created by Alex on 21.05.2017.
 */
public class RegexFilterVisitor extends PredicateBaseVisitor<FilterPredicate> {

    @Override
    public FilterPredicate visitRegex_filter(PredicateParser.Regex_filterContext ctx) {
        String colName = FieldFilterVisitor.getColname(ctx.VAR().getText());
        return FilterPredicate.matches(colName,convertPattern(ctx.REGEX().getText()));
    }
    private static Pattern convertPattern(String text){
        if(!text.startsWith("/") || ! text.endsWith("/")){
            throw new PredicateCompilerException(String.format("wrong pattern format: %s",text));
        }
        text = text.substring(1,text.length()-1);
        return Pattern.compile(text);

    }



}
