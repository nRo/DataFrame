package de.unknownreality.dataframe.column;


import de.unknownreality.dataframe.common.parser.Parser;
import de.unknownreality.dataframe.common.parser.ParserUtil;

/**
 * Created by Alex on 09.03.2016.
 */
public class StringColumn extends BasicColumn<String, StringColumn> {

    public StringColumn() {
        super();
    }

    public StringColumn(String name) {
        super(name);
    }

    public StringColumn(String name, String[] values) {
        super(name, values);
    }

    private final Parser<String> parser = ParserUtil.findParserOrNull(String.class);

    @Override
    public Parser<String> getParser() {
        return parser;
    }

    @Override
    protected StringColumn getThis() {
        return this;
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }

    @Override
    public StringColumn copy() {
        String[] copyValues = new String[size()];
        toArray(copyValues);
        return new StringColumn(getName(), copyValues);
    }
}
