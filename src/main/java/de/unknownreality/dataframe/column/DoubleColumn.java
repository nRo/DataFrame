package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class DoubleColumn extends NumberColumn<Double, DoubleColumn> {
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

    private final Parser<Double> parser = ParserUtil.findParserOrNull(Double.class);

    @Override
    public Parser<Double> getParser() {
        return parser;
    }

    @Override
    protected DoubleColumn getThis() {
        return this;
    }


    @Override
    public DoubleColumn copy() {
        Double[] copyValues = new Double[size()];
        toArray(copyValues);
        return new DoubleColumn(getName(), copyValues);
    }

}
