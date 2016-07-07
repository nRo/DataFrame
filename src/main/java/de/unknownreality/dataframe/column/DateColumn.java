package de.unknownreality.dataframe.column;


import de.unknownreality.dataframe.common.parser.Parser;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Alex on 09.03.2016.
 */
public class DateColumn extends BasicColumn<Date> {
    private DateFormat dateFormat;
    public DateColumn(){
        super();
        this.dateFormat = DateFormat.getDateInstance();
    }
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
        toArray(copyValues);
        return new DateColumn(dateFormat,getName(),copyValues);
    }
}
