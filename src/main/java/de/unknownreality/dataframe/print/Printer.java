package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.io.DataWriter;
import de.unknownreality.dataframe.io.ReadFormat;

import java.io.BufferedWriter;
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
    private String tBottom= "┬";
    private String innerVerticalLine = "│";
    private String innerHorizontalLine = "─";
    private String innerCrossConnection = "┼";



    @Override
    public void write(BufferedWriter writer, DataContainer<?, ?> dataContainer) {
        try {
            StringBuilder contentLineSb = new StringBuilder();
            StringBuilder topLineSb = new StringBuilder();
            StringBuilder lastLineSb = new StringBuilder();
            boolean first = true;
            for (Row row : dataContainer) {
                contentLineSb.setLength(0);
                topLineSb.setLength(0);
                lastLineSb.setLength(0);
                for (int i = 0; i < row.size(); i++) {
                    if (i == 0) {
                        contentLineSb.append(leftLine);
                        topLineSb.append(first ? topLeftCorner : tRight);
                        lastLineSb.append(bottomLeftCorner);

                    }
                    Object v = row.get(i);
                    String content = String.format("%-12.10s", v);
                    contentLineSb.append(content);
                    for (int j = 0; j < content.length(); j++) {
                        topLineSb.append(first ? topLine : innerHorizontalLine);
                        lastLineSb.append(bottomLine);
                    }
                    if (i != row.size() - 1) {
                        contentLineSb.append(innerVerticalLine);
                        topLineSb.append(first ? tBottom : innerCrossConnection);
                        lastLineSb.append(tTop);
                    } else {
                        contentLineSb.append(rightLine);
                        topLineSb.append(first ? topRightCorner : tLeft);
                        lastLineSb.append(bottomRightCorner);
                    }
                }
                writer.write(topLineSb.toString());
                writer.newLine();
                writer.write(contentLineSb.toString());
                writer.newLine();
                first = false;

            }
            writer.write(lastLineSb.toString());
            writer.newLine();
            writer.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
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
