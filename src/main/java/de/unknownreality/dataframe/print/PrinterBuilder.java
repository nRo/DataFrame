package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.io.WriterBuilder;

import java.util.HashMap;
import java.util.Map;

public final class PrinterBuilder implements WriterBuilder<Printer> {
    private String topLeftCorner = "┌";
    private String topRightCorner = "┐";
    private String bottomLeftCorner = "└";
    private String bottomRightCorner = "┘";
    private String topLine = "─";
    private String bottomLine = "─";
    private String leftLine = "│";
    private String rightLine = "│";
    private String innerVerticalLine = "│";
    private String innerHorizontalLine = "─";
    private String innerCrossConnection = "┼";
    private String tLeft = "┤";
    private String tRight = "├";
    private String tTop = "┴";
    private String tBottom = "┬";
    private int defaultColumnWidth = 12;
    private int defaultMaxContentWidth = 10;
    private ValueFormatter defaultValueFormatter = new DefaultValueFormatter();
    private ValueFormatter defaultHeaderFormatter = (v, m) -> "#" + v.toString();
    private ValueFormatter defaultNumberFormatter
            = new DefaultNumberFormatter();
    private Map<Object, ColumnPrintSettings> columnSettings = new HashMap<>();

    private PrinterBuilder() {
    }

    public static PrinterBuilder create() {
        return new PrinterBuilder();
    }

    public PrinterBuilder withColumnSetting(Object header, ColumnPrintSettings setting) {
        columnSettings.put(header, setting);
        return this;
    }

    public PrinterBuilder withColumnSetting(Object header, int width, int contentWidth,
                                            ValueFormatter headerFormatter, ValueFormatter valueFormatter) {
        ColumnPrintSettings settings = new ColumnPrintSettings(header);
        settings.setHeaderFormatter(headerFormatter);
        settings.setValueFormatter(valueFormatter);
        settings.setWidth(width);
        settings.setMaxContentWidth(contentWidth);
        columnSettings.put(header, settings);
        return this;
    }

    public PrinterBuilder withColumnSetting(Object header, int width, ValueFormatter valueFormatter) {
       return withColumnSetting(header, width, width - 2, null, null);
    }


    public PrinterBuilder withColumnWidth(Object header, int width) {
        ColumnPrintSettings settings = columnSettings.computeIfAbsent(
                header, (h) -> new ColumnPrintSettings(header)
        );
        settings.setWidth(width);
        return this;
    }

    public PrinterBuilder withAutoWidth(Object header) {
        ColumnPrintSettings settings = columnSettings.computeIfAbsent(
                header, (h) -> new ColumnPrintSettings(header)
        );
        settings.setAutoWidth(true);
        return this;
    }

    public PrinterBuilder withTTop(String tTop){
        this.tTop = tTop;
        return this;
    }
    public PrinterBuilder withTBottom(String tBottom){
        this.tBottom = tBottom;
        return this;
    }
    public PrinterBuilder withTLeft(String tLeft){
        this.tLeft = tLeft;
        return this;
    }
    public PrinterBuilder withTRight(String tRight){
        this.tRight = tRight;
        return this;
    }
    public PrinterBuilder withColumnContentWidth(Object header, int contentWidth) {
        ColumnPrintSettings settings = columnSettings.computeIfAbsent(
                header, (h) -> new ColumnPrintSettings(header)
        );
        settings.setMaxContentWidth(contentWidth);
        return this;
    }

    public PrinterBuilder withColumnValueFormatter(Object header, ValueFormatter formatter) {
        ColumnPrintSettings settings = columnSettings.computeIfAbsent(
                header, (h) -> new ColumnPrintSettings(header)
        );
        settings.setValueFormatter(formatter);
        return this;
    }

    public PrinterBuilder withColumnHeaderFormatter(Object header, ValueFormatter formatter) {
        ColumnPrintSettings settings = columnSettings.computeIfAbsent(
                header, (h) -> new ColumnPrintSettings(header)
        );
        settings.setHeaderFormatter(formatter);
        return this;
    }


    public PrinterBuilder withTopLeftCorner(String topLeftCorner) {
        this.topLeftCorner = topLeftCorner;
        return this;
    }

    public PrinterBuilder withTopRightCorner(String topRightCorner) {
        this.topRightCorner = topRightCorner;
        return this;
    }

    public PrinterBuilder withBottomLeftCorner(String bottomLeftCorner) {
        this.bottomLeftCorner = bottomLeftCorner;
        return this;
    }

    public PrinterBuilder withBottomRightCorner(String bottomRightCorner) {
        this.bottomRightCorner = bottomRightCorner;
        return this;
    }

    public PrinterBuilder withCorner(String corner){
        withBottomLeftCorner(corner);
        withBottomRightCorner(corner);
        withTopRightCorner(corner);
        withTopLeftCorner(corner);
        return this;
    }
    public PrinterBuilder withVerticalLine(String vline){
        withLeftLine(vline);
        withRightLine(vline);
        withInnerVerticalLine(vline);
        return this;
    }

    public PrinterBuilder withHorizontalLine(String hline){
        withTopLine(hline);
        withBottomLine(hline);
        withInnerHorizontalLine(hline);
        return this;
    }

    public PrinterBuilder withJoint(String joint){
        withInnerCrossConnection(joint);
        withTLeft(joint);
        withTRight(joint);
        withTBottom(joint);
        withTTop(joint);
        return this;
    }

    public PrinterBuilder withTopLine(String topLine) {
        this.topLine = topLine;
        return this;
    }

    public PrinterBuilder withBottomLine(String bottomLine) {
        this.bottomLine = bottomLine;
        return this;
    }

    public PrinterBuilder withLeftLine(String leftLine) {
        this.leftLine = leftLine;
        return this;
    }

    public PrinterBuilder withRightLine(String rightLine) {
        this.rightLine = rightLine;
        return this;
    }

    public PrinterBuilder withInnerVerticalLine(String innerVerticalLine) {
        this.innerVerticalLine = innerVerticalLine;
        return this;
    }

    public PrinterBuilder withInnerHorizontalLine(String innerHorizontalLine) {
        this.innerHorizontalLine = innerHorizontalLine;
        return this;
    }

    public PrinterBuilder withInnerCrossConnection(String innerCrossConnection) {
        this.innerCrossConnection = innerCrossConnection;
        return this;
    }

    public PrinterBuilder withDefaultColumnWidth(int defaultColumnWidth) {
        this.defaultColumnWidth = defaultColumnWidth;
        return this;
    }

    public PrinterBuilder withDefaultMaxContentWidth(int defaultMaxContentWidth) {
        this.defaultMaxContentWidth = defaultMaxContentWidth;
        return this;
    }

    public PrinterBuilder withDefaultValueFormatter(ValueFormatter defaultValueFormatter) {
        this.defaultValueFormatter = defaultValueFormatter;
        return this;
    }

    public PrinterBuilder withDefaultHeaderFormatter(ValueFormatter defaultHeaderFormatter) {
        this.defaultHeaderFormatter = defaultHeaderFormatter;
        return this;
    }

    public PrinterBuilder withDefaultNumberFormatter(ValueFormatter defaultNumberFormatter) {
        this.defaultNumberFormatter = defaultNumberFormatter;
        return this;
    }

    @Override
    public Printer build() {
        Printer printer = new Printer();
        printer.setTopLeftCorner(topLeftCorner);
        printer.setTopRightCorner(topRightCorner);
        printer.setBottomLeftCorner(bottomLeftCorner);
        printer.setBottomRightCorner(bottomRightCorner);
        printer.setTopLine(topLine);
        printer.setBottomLine(bottomLine);
        printer.setLeftLine(leftLine);
        printer.setRightLine(rightLine);
        printer.settTop(tTop);
        printer.settRight(tRight);
        printer.settLeft(tLeft);
        printer.settBottom(tBottom);
        printer.setInnerVerticalLine(innerVerticalLine);
        printer.setInnerHorizontalLine(innerHorizontalLine);
        printer.setInnerCrossConnection(innerCrossConnection);
        printer.setDefaultColumnWidth(defaultColumnWidth);
        printer.setDefaultMaxContentWidth(defaultMaxContentWidth);
        printer.setDefaultValueFormatter(defaultValueFormatter);
        printer.setDefaultHeaderFormatter(defaultHeaderFormatter);
        printer.setDefaultNumberFormatter(defaultNumberFormatter);
        printer.getColumnSettings().putAll(columnSettings);
        return printer;
    }
}
