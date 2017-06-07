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

import java.util.regex.Pattern;

/**
 * Created by Alex on 21.05.2017.
 */
public class ColumnPredicateFilterVisitor extends PredicateBaseVisitor<FilterPredicate> {

    @Override
    public FilterPredicate visitColumn_predicate(PredicateParser.Column_predicateContext ctx) {
        String colNameA = FieldFilterVisitor.getColname(ctx.COLUMN(0).getText());
        String colNameB = FieldFilterVisitor.getColname(ctx.COLUMN(1).getText());
        return createColumnFieldFilter(colNameA,colNameB,ctx.FIELD_OPERATION().getText());
    }

    private static FilterPredicate createColumnFieldFilter(String colNameA, String colNameB, String operation) {
        FieldFilterOperation fieldFilterOperation = FieldFilterOperation.find(operation);

        switch (fieldFilterOperation) {
            case EQ:
                return FilterPredicate.eqColumn(colNameA, colNameB);
            case NE:
                return FilterPredicate.neColumn(colNameA, colNameB);
            case LE:
                return FilterPredicate.leColumn(colNameA, colNameB);
            case LT:
                return FilterPredicate.ltColumn(colNameA, colNameB);
            case GE:
                return FilterPredicate.geColumn(colNameA, colNameB);
            case GT:
                return FilterPredicate.gtColumn(colNameA, colNameB);

            default:
                throw new PredicateCompilerException(String.format("unsupported filter operation '%s'", operation));

        }
    }



}
