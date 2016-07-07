package de.unknownreality.dataframe.column;

import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Alex on 09.03.2016.
 */
public class ByteColumn extends NumberColumn<Byte, ByteColumn> {
    private static Logger log = LoggerFactory.getLogger(ByteColumn.class);

    public ByteColumn() {
        super();
    }

    public ByteColumn(String name) {
        super(name);
    }

    public ByteColumn(String name, Byte[] values) {
        super(name, values);
    }


    @Override
    public Class<Byte> getType() {
        return Byte.class;
    }

    private final Parser<Byte> parser = ParserUtil.findParserOrNull(Byte.class);

    @Override
    public Parser<Byte> getParser() {
        return parser;
    }

    @Override
    protected ByteColumn getThis() {
        return this;
    }

    @Override
    public ByteColumn copy() {
        Byte[] copyValues = new Byte[size()];
        toArray(copyValues);
        return new ByteColumn(getName(), copyValues);
    }

}
