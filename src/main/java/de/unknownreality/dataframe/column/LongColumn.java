package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class LongColumn extends NumberColumn<Long, LongColumn> {
    private static Logger log = LoggerFactory.getLogger(LongColumn.class);

    public LongColumn() {
        super();
    }

    public LongColumn(String name) {
        super(name);
    }

    public LongColumn(String name, Long[] values) {
        super(name, values);
    }


    @Override
    protected LongColumn getThis() {
        return this;
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    private final Parser<Long> parser = ParserUtil.findParserOrNull(Long.class);

    @Override
    public Parser<Long> getParser() {
        return parser;
    }


    @Override
    public LongColumn copy() {
        Long[] copyValues = new Long[size()];
        toArray(copyValues);
        return new LongColumn(getName(), copyValues);
    }

}
