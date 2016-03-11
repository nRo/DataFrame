package de.unknownreality.data.frame.column;


import de.unknownreality.data.common.parser.Parser;

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

    @Override
    public Parser<String> getParser() {
        return new Parser() {
            @Override
            public Object parse(String s) {
                return s;
            }
        };
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }
    @Override
    public StringColumn copy() {
        String[] copyValues = new String[size()];
        System.arraycopy(getValues(),0,copyValues,0,getValues().length);
        return new StringColumn(getName(),copyValues);
    }
}
