package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.ReadFormat;

import java.io.BufferedWriter;
import java.io.IOException;
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


    @Override
    public void write(BufferedWriter writer, DataContainer<?, ?> dataContainer) {
        try {
            writeHeader(dataContainer, writer);
            StringBuilder contentLineSb = new StringBuilder();
            StringBuilder topLineSb = new StringBuilder();
            StringBuilder lastLineSb = new StringBuilder();
            boolean last;
            Iterator<? extends Row> it = dataContainer.iterator();
            while (it.hasNext()) {
                Row<?,?> row = it.next();
                contentLineSb.setLength(0);
                topLineSb.setLength(0);
                last = !it.hasNext();
                for (int i = 0; i < row.size(); i++) {
                    if (i == 0) {
                        contentLineSb.append(leftLine);
                        topLineSb.append(tRight);
                        if (last) {
                            lastLineSb.append(bottomLeftCorner);
                        }
                    }
                    String content = formatContent(row, i);
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
            e.printStackTrace();
        }
    }

    private String formatContent(Row<?, ?> row, int col) {
        Object v = row.get(col);
        return String.format("%-12.10s", v);
    }

    private String formatHeaderContent(Object header) {
        return String.format("#%-11.10s", header);
    }


    protected void writeHeader(DataContainer<?, ?> dataContainer, BufferedWriter writer) throws IOException {
        StringBuilder contentLineSb = new StringBuilder();
        StringBuilder topLineSb = new StringBuilder();
        topLineSb.append(topLeftCorner);
        int cols = dataContainer.getHeader().size();
        int cidx = 0;
        for (Object header : dataContainer.getHeader()) {
            if (contentLineSb.length() == 0) {
                contentLineSb.append(leftLine);
            }
            String content = formatHeaderContent(header);
            contentLineSb.append(content);
            for (int i = 0; i < content.length(); i++) {
                topLineSb.append(topLine);
            }
            if (cidx < cols - 1) {
                contentLineSb.append(innerVerticalLine);
                topLineSb.append(tBottom);
            } else {
                topLineSb.append(topRightCorner);
                contentLineSb.append(rightLine);
            }
            cidx++;
        }
        writer.write(topLineSb.toString());
        writer.newLine();
        writer.write(contentLineSb.toString());
        writer.newLine();
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
