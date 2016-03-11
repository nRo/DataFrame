package de.unknownreality.data.frame.column;


import de.unknownreality.data.common.parser.Parser;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Alex on 09.03.2016.
 */
public class DateColumn extends BasicColumn<Date> {
    private DateFormat dateFormat;
    public DateColumn(DateFormat dateFormat){
        super();
        this.dateFormat = dateFormat;
    }
    public DateColumn(DateFormat dateFormat,String name) {
        super(name);
        this.dateFormat = dateFormat;
    }

    public DateColumn(DateFormat dateFormat,String name, Date[] values) {
        super(name,values);
        this.dateFormat = dateFormat;
    }

    @Override
    public Parser<Date> getParser() {
        return new Parser<Date>() {
            @Override
            public Date parse(String s) throws ParseException{
                return dateFormat.parse(s);
            }
        };
    }

    @Override
    public Class<Date> getType() {
        return Date.class;
    }
    @Override
    public DateColumn copy() {
        Date[] copyValues = new Date[size()];
        System.arraycopy(getValues(),0,copyValues,0,getValues().length);
        return new DateColumn(dateFormat,getName(),copyValues);
    }
}
