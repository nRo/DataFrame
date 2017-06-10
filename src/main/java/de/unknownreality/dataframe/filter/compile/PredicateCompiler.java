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
        PredicateCompileErrorListener errorListener = new PredicateCompileErrorListener(predicateString);

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
