package de.unknownreality.dataframe.join;

/**
 * Created by Alex on 12.03.2016.
 */
public class JoinColumn {
    private String columnA;
    private String columnB;
    public JoinColumn(String columnA,String columnB){
        this.columnA = columnA;
        this.columnB = columnB;
    }
    public JoinColumn(String column){
        this(column,column);
    }

    public String getColumnA() {
        return columnA;
    }

    public String getColumnB() {
        return columnB;
    }
}
