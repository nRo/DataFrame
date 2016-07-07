package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class IntegerColumn extends NumberColumn<Integer, IntegerColumn> {
    private static Logger log = LoggerFactory.getLogger(IntegerColumn.class);

    public IntegerColumn() {
        super();
    }

    public IntegerColumn(String name) {
        super(name);
    }

    public IntegerColumn(String name, Integer[] values) {
        super(name, values);
    }


    @Override
    protected IntegerColumn getThis() {
        return null;
    }

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }

    private final Parser<Integer> parser = ParserUtil.findParserOrNull(Integer.class);

    @Override
    public Parser<Integer> getParser() {
        return parser;
    }

    @Override
    public IntegerColumn copy() {
        Integer[] copyValues = new Integer[size()];
        toArray(copyValues);
        return new IntegerColumn(getName(), copyValues);
    }

}
