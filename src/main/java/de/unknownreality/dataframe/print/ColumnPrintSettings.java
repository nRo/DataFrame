package de.unknownreality.dataframe.print;

public class ColumnPrintSettings {
    private Object columnHeader;
    private ValueFormatter<Object> valueFormatter;
    private ValueFormatter<Object> headerFormatter;
    private Integer width;
    private Integer maxContentWidth;


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

    public ValueFormatter<Object> getValueFormatter() {
        return valueFormatter;
    }

    public void setValueFormatter(ValueFormatter<Object> valueFormatter) {
        this.valueFormatter = valueFormatter;
    }

    public Object getColumnHeader() {
        return columnHeader;
    }

    public void setColumnHeader(Object columnHeader) {
        this.columnHeader = columnHeader;
    }

    public ValueFormatter<Object> getHeaderFormatter() {
        return headerFormatter;
    }

    public void setHeaderFormatter(ValueFormatter<Object> headerFormatter) {
        this.headerFormatter = headerFormatter;
    }
}
