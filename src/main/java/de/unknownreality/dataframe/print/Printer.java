package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameWriter;
import de.unknownreality.dataframe.DataRow;
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
        try{
            StringBuilder line = new StringBuilder();
            StringBuilder topLine = new StringBuilder();
            StringBuilder lastLine = new StringBuilder();
            boolean first = true;
            for(Row row :dataContainer){
                line.setLength(0);
                topLine.setLength(0);
                lastLine.setLength(0);
                for(int i = 0; i < row.size(); i++){
                    if(i == 0){
                        line.append(leftLine);
                        topLine.append(first ? topLeftCorner : tRight);
                        lastLine.append(bottomLeftCorner);

                    }
                    Object v = row.get(i);
                    String content = String.format("%-12.10s",v);
                    line.append(content);
                    for(int j = 0; j < content.length(); j++){
                        topLine.append(first ? topLine : innerHorizontalLine);
                        lastLine.append(bottomLine);
                    }
                    if(i != row.size() - 1){
                        line.append(innerVerticalLine);
                        topLine.append(first ? tBottom : innerCrossConnection);
                        lastLine.append(tTop);
                    }
                    else{
                        line.append(rightLine);
                        topLine.append(first ? topRightCorner : tLeft);
                        lastLine.append(bottomRightCorner);
                    }
                }
                writer.write(topLine.toString());
                writer.newLine();
                writer.write(line.toString());
                writer.newLine();
                first = false;

            }
            writer.write(lastLine.toString());
            writer.newLine();
            writer.flush();

        }
        catch (Exception e){
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
