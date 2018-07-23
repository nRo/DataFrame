package de.unknownreality.dataframe.print;

public class ColumnPrintSettings {
    private Object columnHeader;
    private ValueFormatter valueFormatter;
    private ValueFormatter headerFormatter;
    private Integer width;
    private Integer maxContentWidth;

    public ColumnPrintSettings(Object columnHeader){
        this.columnHeader = columnHeader;
    }
    public ColumnPrintSettings(){};

    public Integer getMaxContentWidth() {
        return maxContentWidth;
    }

    public void setMaxContentWidth(Integer maxContentWidth) {
        this.maxContentWidth = maxContentWidth;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Object getColumnName() {
        return columnHeader;
    }

    public void setColumnName(Object columnName) {
        this.columnHeader = columnName;
    }

    public ValueFormatter getValueFormatter() {
        return valueFormatter;
    }

    public void setValueFormatter(ValueFormatter valueFormatter) {
        this.valueFormatter = valueFormatter;
    }

    public Object getColumnHeader() {
        return columnHeader;
    }

    public void setColumnHeader(Object columnHeader) {
        this.columnHeader = columnHeader;
    }

    public ValueFormatter getHeaderFormatter() {
        return headerFormatter;
    }

    public void setHeaderFormatter(ValueFormatter headerFormatter) {
        this.headerFormatter = headerFormatter;
    }
}
