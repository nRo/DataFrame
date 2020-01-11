/*
 *
 *  * Copyright (c) 2019 Alexander Grün
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

import java.util.regex.Pattern;

/**
 * Created by Alex on 21.05.2017.
 */
public class RegexFilterVisitor extends PredicateBaseVisitor<FilterPredicate> {

    @Override
    public FilterPredicate visitRegex_filter(PredicateParser.Regex_filterContext ctx) {
        String colName = FieldFilterVisitor.getColname(ctx.variable().getText());
        return FilterPredicate.matches(colName,convertPattern(ctx.REGEX().getText()));
    }
    private static Pattern convertPattern(String text){
        String regex = text;
        if(!regex.startsWith("/") || ! regex.endsWith("/")){
            throw new PredicateCompilerException(String.format("wrong pattern format: %s",text));
        }
        regex = regex.substring(1,regex.length()-1);
        return Pattern.compile(regex);

    }



}
