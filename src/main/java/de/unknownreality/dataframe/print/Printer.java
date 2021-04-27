/*
 *
 *  * Copyright (c) 2019 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.column.settings.ColumnSettings;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.common.header.Header;
import de.unknownreality.dataframe.common.header.TypeHeader;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.ReadFormat;
import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.ValueType;
import de.unknownreality.dataframe.type.impl.StringType;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//TODO value type
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
    private final int maxAutoWidth = 500;
    private final Map<Object, ColumnPrintSettings> columnSettings = new HashMap<>();
    private ValueFormatter defaultValueFormatter = new DefaultValueFormatter();
    private ValueFormatter defaultHeaderFormatter = (t, v, m) -> "#" + v.toString();
    private final StringType headerType = new StringType(new ColumnSettings());
    private ValueFormatter defaultNumberFormatter
            = new DefaultNumberFormatter();


    @Override
    public void write(BufferedWriter writer, DataContainer<?, ?> dataContainer) {
        try {
            ColumnPrintSettings[] settings = createPrintSettings(dataContainer);
            ColumnWidth[] columnWidths = createColumnWidth(dataContainer, settings);
            writeHeader(dataContainer, writer, settings, columnWidths);
            StringBuilder contentLineSb = new StringBuilder();
            StringBuilder topLineSb = new StringBuilder();
            StringBuilder lastLineSb = new StringBuilder();
            boolean last;
            Iterator<? extends Row<?, ?>> it = dataContainer.iterator();
            while (it.hasNext()) {
                Row<?, ?> row = it.next();
                contentLineSb.setLength(0);
                topLineSb.setLength(0);
                last = !it.hasNext();
                for (int i = 0; i < row.size(); i++) {
                    ColumnPrintSettings colSettings = settings[i];
                    ColumnWidth columnWidth = columnWidths[i];
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
                    String content = formatContent(colSettings, columnWidth, row, i);
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
            throw new DataFrameRuntimeException("error printing data container", e);
        }
    }

    private ColumnWidth[] createColumnWidth(DataContainer<?, ?> dataContainer, ColumnPrintSettings[] columnPrintSetting) {
        ColumnWidth[] widths = new ColumnWidth[dataContainer.getHeader().size()];
        int[] autoWidthCols = new int[widths.length];
        int[] max = new int[widths.length];

        int autoWidthColsCount = 0;
        for (int i = 0; i < widths.length; i++) {
            ColumnPrintSettings settings = columnPrintSetting[i];
            ColumnWidth columnWidth = new ColumnWidth();
            if (!settings.isAutoWidth()) {
                columnWidth.width = settings.getWidth();
                columnWidth.contentWidth = settings.getMaxContentWidth();
            } else {
                max[i] = getContentLength(headerType, dataContainer.getHeader().get(i), settings);
                autoWidthCols[autoWidthColsCount++] = i;
            }
            widths[i] = columnWidth;
        }
        if (autoWidthColsCount == 0) {
            return widths;
        }
        int c;
        for (Row<?, ?> row : dataContainer) {
            for (int i = 0; i < autoWidthColsCount; i++) {
                c = autoWidthCols[i];
                ColumnPrintSettings settings = columnPrintSetting[c];

                Object v = row.get(c);
                int length = getContentLength(row.getType(c), v, settings);
                max[i] = Math.max(max[i], length);
            }
        }
        for (int i = 0; i < autoWidthColsCount; i++) {
            ColumnWidth columnWidth = widths[autoWidthCols[i]];
            int m = Math.min(maxAutoWidth, max[i]);
            columnWidth.contentWidth = m;
            columnWidth.width = m + 1;
        }
        return widths;
    }

    private int getContentLength(ValueType<?> type, Object v, ColumnPrintSettings printSettings) {
        int length;
        if (v == null) {
            return 0;
        } else if (Values.NA.isNA(v)) {
            length = 2;
        } else {
            length = printSettings
                    .getValueFormatter().format(type, v, maxAutoWidth).length();
        }
        return length;
    }

    private String formatContent(ColumnPrintSettings columnPrintSettings, ColumnWidth columnWidth, Row<?, ?> row, int col) {
        Object v = row.get(col);
        ValueType<?> type = row.getType(col);
        String valueString;
        if (v == null) {
            valueString = "";
        } else if (Values.NA.isNA(v)) {
            valueString = "NA";
        } else {
            valueString = columnPrintSettings
                    .getValueFormatter().format(type, v,
                            columnWidth.contentWidth);
        }
        String fmt = "%-" + columnWidth.width + "." + columnWidth.contentWidth + "s";
        return String.format(fmt, valueString);
    }

    private String formatHeaderContent(Object header, ColumnPrintSettings columnPrintSettings, ColumnWidth columnWidth) {
        String headerString = columnPrintSettings
                .getHeaderFormatter().format(headerType, header,
                        columnWidth.contentWidth);

        String fmt = "%-" + columnWidth.width + "." + columnWidth.contentWidth + "s";
        return String.format(fmt, headerString);
    }

    protected ColumnPrintSettings[] createPrintSettings(DataContainer<?, ?> dataContainer) {
        ColumnPrintSettings[] settings = new ColumnPrintSettings[dataContainer.getHeader().size()];
        for (int i = 0; i < dataContainer.getHeader().size(); i++) {
            ColumnPrintSettings columnSettings = getSettings(dataContainer, i);
            settings[i] = columnSettings;
        }
        return settings;
    }

    protected void writeHeader(DataContainer<?, ?> dataContainer, BufferedWriter writer, ColumnPrintSettings[] settings, ColumnWidth[] columnWidths) throws IOException {
        StringBuilder contentLineSb = new StringBuilder();
        StringBuilder topLineSb = new StringBuilder();
        topLineSb.append(topLeftCorner);
        int cols = dataContainer.getHeader().size();
        int colIdx = 0;
        for (Object header : dataContainer.getHeader()) {
            ColumnWidth columnWidth = columnWidths[colIdx];
            ColumnPrintSettings columnSettings = settings[colIdx];
            if (contentLineSb.length() == 0) {
                contentLineSb.append(leftLine);
            }
            String content = formatHeaderContent(header, columnSettings, columnWidth);
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
        if (header instanceof TypeHeader<?>) {
            TypeHeader<?> typeHeader = (TypeHeader<?>) header;
            Class<?> type = typeHeader.getValueType(colIdx).getType();
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
    public List<DataFrameColumn<?, ?>> getMetaColumns(DataFrame dataFrame) {
        return null;
    }

    @Override
    public ReadFormat<?, ?> getReadFormat() {
        return null;
    }

    private static class ColumnWidth {
        public int contentWidth;
        public int width;
    }
}
