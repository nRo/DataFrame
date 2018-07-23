package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.NumberUtil;

import java.util.Locale;

public class DefaultNumberFormatter implements ValueFormatter {
    @Override
    public String format(Object value, int maxWidth) {
        if(!(value instanceof Number)){
            throw new DataFrameRuntimeException(String.format("number type expected (%s)",value.getClass()));
        }
        Number num = (Number) value;
        if(!NumberUtil.isFloatOrDouble(num)){
            return String.format("%."+maxWidth+"s", num.longValue());
        }
        else{
            int n = maxWidth - 2;
            return String.format(Locale.US,"%."+n+"f", num.doubleValue());
        }
    }


}
