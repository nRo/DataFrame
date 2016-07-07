package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class FloatColumn extends NumberColumn<Float, FloatColumn> {
    private static Logger log = LoggerFactory.getLogger(FloatColumn.class);

    public FloatColumn() {
        super();
    }

    public FloatColumn(String name) {
        super(name);
    }

    public FloatColumn(String name, Float[] values) {
        super(name, values);
    }


    @Override
    protected FloatColumn getThis() {
        return null;
    }


    @Override
    public Class<Float> getType() {
        return Float.class;
    }

    private final Parser<Float> parser = ParserUtil.findParserOrNull(Float.class);


    @Override
    public Parser<Float> getParser() {
        return parser;
    }


    @Override
    public FloatColumn copy() {
        Float[] copyValues = new Float[size()];
        toArray(copyValues);
        return new FloatColumn(getName(), copyValues);
    }

}
