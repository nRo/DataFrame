package de.unknownreality.data.frame.column;


import de.unknownreality.data.common.parser.Parser;
import de.unknownreality.data.common.parser.ParserUtil;

/**
 * Created by Alex on 09.03.2016.
 */
public class StringColumn extends BasicColumn<String> {

    public StringColumn(){
        super();
    }
    public StringColumn(String name) {
        super(name);
    }

    public StringColumn(String name,String[] values) {
        super(name,values);
    }

    private Parser parser = ParserUtil.findParserOrNull(String.class);
    @Override
    public Parser<String> getParser() {
        return parser;
    }


    @Override
    public Class<String> getType() {
        return String.class;
    }
    @Override
    public StringColumn copy() {
        String[] copyValues = new String[size()];
        System.arraycopy(getValues(),0,copyValues,0,size());
        return new StringColumn(getName(),copyValues);
    }
}
