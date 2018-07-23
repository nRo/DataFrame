package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Header;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.header.BasicTypeHeader;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.ReadFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Printer extends DataWriter {
    private String topLeftCorner = "┌";
    private String topRightCorner = "┐";
    private String bottomLeftCorner = "└";
    private String bottomRightCorner = "┘";
    private String topLine = "─";
    private String bottomLine = "─";
    private String leftLine = "│";
    private String rightLine = "│";
    private String tLeft = "┤";
    private String tRight = "├";
    private String tTop = "┴";
    private String tBottom = "┬";
    private String innerVerticalLine = "│";
    private String innerHorizontalLine = "─";
    private String innerCrossConnection = "┼";
    private int defaultColumnWidth = 12;
    private int defaultMaxContentWidth = 10;
    private Map<Object, ColumnPrintSettings> columnSettings = new HashMap<>();
    private ValueFormatter defaultValueFormatter =
            (value, maxWidth) -> String.format("%."+maxWidth+"s", value);
    private ValueFormatter defaultHeaderFormatter = (v, m) -> "#" + v.toString();
    private ValueFormatter defaultNumberFormatter
            = new DefaultNumberFormatter();

    @Override
    public void write(BufferedWriter writer, DataContainer<?, ?> dataContainer) {
        try {
            ColumnPrintSettings[] settings = new ColumnPrintSettings[dataContainer.getHeader().size()];
            writeHeader(dataContainer, writer, settings);
            StringBuilder contentLineSb = new StringBuilder();
            StringBuilder topLineSb = new StringBuilder();
            StringBuilder lastLineSb = new StringBuilder();
            boolean last;
            Iterator<? extends Row> it = dataContainer.iterator();
            while (it.hasNext()) {
                Row<?, ?> row = it.next();
                contentLineSb.setLength(0);
                topLineSb.setLength(0);
                last = !it.hasNext();
                for (int i = 0; i < row.size(); i++) {
                    ColumnPrintSettings colSettings = settings[i];
                    if (colSettings.getValueFormatter() == null) {
                        colSettings.setValueFormatter(getDefaultValueFormatter(row.get(i)));
                    }
                    if (i == 0) {
                        contentLineSb.append(leftLine);
                        topLineSb.append(tRight);
                        if (last) {
                            lastLineSb.append(bottomLeftCorner);
                        }
                    }
                    String content = formatContent(colSettings, row, i);
                    contentLineSb.append(content);
                    for (int j = 0; j < content.length(); j++) {
                        topLineSb.append(innerHorizontalLine);
                        if (last) {
                            lastLineSb.append(bottomLine);
                        }
                    }
                    if (i != row.size() - 1) {
                        contentLineSb.append(innerVerticalLine);
                        topLineSb.append(innerCrossConnection);
                        if (last) {
                            lastLineSb.append(tTop);
                        }
                    } else {
                        contentLineSb.append(rightLine);
                        topLineSb.append(tLeft);
                        if (last) {
                            lastLineSb.append(bottomRightCorner);
                        }
                    }
                }
                writer.write(topLineSb.toString());
                writer.newLine();

                writer.write(contentLineSb.toString());
                writer.newLine();

                if (last) {
                    writer.write(lastLineSb.toString());
                    writer.newLine();
                }

            }
            writer.flush();

        } catch (Exception e) {
            throw new DataFrameRuntimeException("error printing data container",e);
        }
    }

    private String formatContent(ColumnPrintSettings columnPrintSettings, Row<?, ?> row, int col) {
        Object v = row.get(col);
        String valueString = columnPrintSettings
                .getValueFormatter().format(v,
                        columnPrintSettings.getMaxContentWidth());
        String fmt = "%-" + columnPrintSettings.getWidth() + "." + columnPrintSettings.getMaxContentWidth() + "s";
        return String.format(fmt, valueString);
    }

    private String formatHeaderContent(Object header, ColumnPrintSettings columnPrintSettings) {
        String headerString = columnPrintSettings
                .getHeaderFormatter().format(header,
                        columnPrintSettings.getMaxContentWidth());

        String fmt = "%-" + columnPrintSettings.getWidth() + "." + columnPrintSettings.getMaxContentWidth() + "s";
        return String.format(fmt, headerString);
    }


    protected void writeHeader(DataContainer<?, ?> dataContainer, BufferedWriter writer, ColumnPrintSettings[] settings) throws IOException {
        StringBuilder contentLineSb = new StringBuilder();
        StringBuilder topLineSb = new StringBuilder();
        topLineSb.append(topLeftCorner);
        int cols = dataContainer.getHeader().size();
        int colIdx = 0;
        for (Object header : dataContainer.getHeader()) {
            ColumnPrintSettings columnSettings = getSettings(dataContainer, colIdx);
            settings[colIdx] = columnSettings;
            if (contentLineSb.length() == 0) {
                contentLineSb.append(leftLine);
            }
            String content = formatHeaderContent(header, columnSettings);
            contentLineSb.append(content);
            for (int i = 0; i < content.length(); i++) {
                topLineSb.append(topLine);
            }
            if (colIdx < cols - 1) {
                contentLineSb.append(innerVerticalLine);
                topLineSb.append(tBottom);
            } else {
                topLineSb.append(topRightCorner);
                contentLineSb.append(rightLine);
            }
            colIdx++;
        }
        writer.write(topLineSb.toString());
        writer.newLine();
        writer.write(contentLineSb.toString());
        writer.newLine();
    }

    private ColumnPrintSettings getSettings(DataContainer<?, ?> dataContainer, int colIdx) {
        Object header = dataContainer.getHeader().get(colIdx);
        if (!columnSettings.containsKey(header)) {
            return getDefaultSettings(dataContainer, colIdx);
        }
        ColumnPrintSettings settings = columnSettings.get(header);
        if (settings.getValueFormatter() == null) {
            settings.setValueFormatter(
                    getDefaultValueFormatter(dataContainer, colIdx)
            );
        }
        if (settings.getHeaderFormatter() == null) {
            settings.setHeaderFormatter(
                    getDefaultHeaderFormatter(dataContainer, colIdx)
            );
        }
        if (settings.getWidth() == null) {
            settings.setWidth(defaultColumnWidth);
        }
        if (settings.getMaxContentWidth() == null) {
            settings.setMaxContentWidth(defaultMaxContentWidth);
        }
        return settings;
    }

    private ColumnPrintSettings getDefaultSettings(DataContainer<?, ?> dataContainer, int colIdx) {
        ColumnPrintSettings defaultSettings = new ColumnPrintSettings();
        defaultSettings.setMaxContentWidth(defaultMaxContentWidth);
        defaultSettings.setWidth(defaultColumnWidth);
        defaultSettings.setValueFormatter(getDefaultValueFormatter(dataContainer, colIdx));
        defaultSettings.setHeaderFormatter(defaultHeaderFormatter);
        return defaultSettings;
    }

    private ValueFormatter getDefaultHeaderFormatter(DataContainer<?, ?> dataContainer, int colIdx) {
        return defaultHeaderFormatter;
    }

    private ValueFormatter getDefaultValueFormatter(DataContainer<?, ?> dataContainer, int colIdx) {
        Header<?> header = dataContainer.getHeader();
        if (header instanceof BasicTypeHeader<?>) {
            BasicTypeHeader<?> typeHeader = (BasicTypeHeader<?>) header;
            Class<?> type = typeHeader.getType(colIdx);
            return getDefaultValueFormatter(type);
        }
        return null;
    }

    private ValueFormatter getDefaultValueFormatter(Object val) {
        if (val instanceof Number && defaultNumberFormatter != null) {
            return defaultNumberFormatter;
        }
        return defaultValueFormatter;
    }

    private ValueFormatter getDefaultValueFormatter(Class<?> type) {
        if (Number.class.isAssignableFrom(type) && defaultNumberFormatter != null) {
            return defaultNumberFormatter;
        }
        return defaultValueFormatter;
    }

    public String getTopLeftCorner() {
        return topLeftCorner;
    }

    public void setTopLeftCorner(String topLeftCorner) {
        this.topLeftCorner = topLeftCorner;
    }

    public String getTopRightCorner() {
        return topRightCorner;
    }

    public void setTopRightCorner(String topRightCorner) {
        this.topRightCorner = topRightCorner;
    }

    public String getBottomLeftCorner() {
        return bottomLeftCorner;
    }

    public void setBottomLeftCorner(String bottomLeftCorner) {
        this.bottomLeftCorner = bottomLeftCorner;
    }

    public String getBottomRightCorner() {
        return bottomRightCorner;
    }

    public void setBottomRightCorner(String bottomRightCorner) {
        this.bottomRightCorner = bottomRightCorner;
    }

    public String getTopLine() {
        return topLine;
    }

    public void setTopLine(String topLine) {
        this.topLine = topLine;
    }

    public String getBottomLine() {
        return bottomLine;
    }

    public void setBottomLine(String bottomLine) {
        this.bottomLine = bottomLine;
    }

    public String getLeftLine() {
        return leftLine;
    }

    public void setLeftLine(String leftLine) {
        this.leftLine = leftLine;
    }

    public String getRightLine() {
        return rightLine;
    }

    public void setRightLine(String rightLine) {
        this.rightLine = rightLine;
    }

    public String gettLeft() {
        return tLeft;
    }

    public void settLeft(String tLeft) {
        this.tLeft = tLeft;
    }

    public String gettRight() {
        return tRight;
    }

    public void settRight(String tRight) {
        this.tRight = tRight;
    }

    public String gettTop() {
        return tTop;
    }

    public void settTop(String tTop) {
        this.tTop = tTop;
    }

    public String gettBottom() {
        return tBottom;
    }

    public void settBottom(String tBottom) {
        this.tBottom = tBottom;
    }

    public String getInnerVerticalLine() {
        return innerVerticalLine;
    }

    public void setInnerVerticalLine(String innerVerticalLine) {
        this.innerVerticalLine = innerVerticalLine;
    }

    public String getInnerHorizontalLine() {
        return innerHorizontalLine;
    }

    public void setInnerHorizontalLine(String innerHorizontalLine) {
        this.innerHorizontalLine = innerHorizontalLine;
    }

    public String getInnerCrossConnection() {
        return innerCrossConnection;
    }

    public void setInnerCrossConnection(String innerCrossConnection) {
        this.innerCrossConnection = innerCrossConnection;
    }

    public int getDefaultColumnWidth() {
        return defaultColumnWidth;
    }

    public void setDefaultColumnWidth(int defaultColumnWidth) {
        this.defaultColumnWidth = defaultColumnWidth;
    }

    public int getDefaultMaxContentWidth() {
        return defaultMaxContentWidth;
    }

    public void setDefaultMaxContentWidth(int defaultMaxContentWidth) {
        this.defaultMaxContentWidth = defaultMaxContentWidth;
    }

    public ValueFormatter getDefaultValueFormatter() {
        return defaultValueFormatter;
    }

    public void setDefaultValueFormatter(ValueFormatter defaultValueFormatter) {
        this.defaultValueFormatter = defaultValueFormatter;
    }

    public ValueFormatter getDefaultHeaderFormatter() {
        return defaultHeaderFormatter;
    }

    public void setDefaultHeaderFormatter(ValueFormatter defaultHeaderFormatter) {
        this.defaultHeaderFormatter = defaultHeaderFormatter;
    }

    public ValueFormatter getDefaultNumberFormatter() {
        return defaultNumberFormatter;
    }

    public void setDefaultNumberFormatter(ValueFormatter defaultNumberFormatter) {
        this.defaultNumberFormatter = defaultNumberFormatter;
    }

    public Map<Object, ColumnPrintSettings> getColumnSettings() {
        return columnSettings;
    }

    @Override
    public Map<String, String> getSettings(DataFrame dataFrame) {
        return null;
    }

    @Override
    public List<DataFrameColumn> getMetaColumns(DataFrame dataFrame) {
        return null;
    }

    @Override
    public ReadFormat getReadFormat() {
        return null;
    }
}
