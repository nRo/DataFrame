package de.unknownreality.dataframe.frame.filter;

import de.unknownreality.dataframe.common.Row;

import java.util.regex.Pattern;

/**
 * Created by Alex on 09.03.2016.
 */
public class MatchPredicate extends FilterPredicate{
    private Pattern pattern;
    private String headerName;

    public MatchPredicate(String headerName, String patternString){
        this(headerName,Pattern.compile(patternString));
    }
    public MatchPredicate(String headerName, Pattern pattern){
        this.headerName = headerName;
        this.pattern = pattern;
    }


    @Override
    public boolean valid(Row row) {
        Object v = row.get(headerName);
        return pattern.matcher(v.toString()).matches();
    }

    @Override
    public String toString() {
        return headerName+" =~ /"+pattern.toString()+"/";
    }
}
