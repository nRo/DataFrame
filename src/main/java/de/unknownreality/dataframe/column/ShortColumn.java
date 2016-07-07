package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class ShortColumn extends NumberColumn<Short, ShortColumn> {
    private static Logger log = LoggerFactory.getLogger(ShortColumn.class);

    public ShortColumn() {
        super();
    }

    public ShortColumn(String name) {
        super(name);
    }

    public ShortColumn(String name, Short[] values) {
        super(name, values);
    }


    @Override
    public Class<Short> getType() {
        return Short.class;
    }

    private final Parser<Short> parser = ParserUtil.findParserOrNull(Short.class);

    @Override
    public Parser<Short> getParser() {
        return parser;
    }

    @Override
    protected ShortColumn getThis() {
        return this;
    }

    @Override
    public ShortColumn copy() {
        Short[] copyValues = new Short[size()];
        toArray(copyValues);
        return new ShortColumn(getName(), copyValues);
    }

}
