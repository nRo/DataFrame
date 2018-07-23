package de.unknownreality.dataframe.print;



public class DefaultValueFormatter implements ValueFormatter {
    @Override
    public String format(Object value, int maxWidth) {
        if((value instanceof String)){
            String strVal = (String) value;
            if(strVal.length() > maxWidth){
                value = strVal.substring(0,maxWidth - 3);
                value += "...";
            }
        }
        return String.format("%."+maxWidth+"s", value);

    }


}
