package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.ColumnTypeMap;
import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.print.Printer;
import de.unknownreality.dataframe.print.PrinterBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;

public class PrintTest {
    @Test
    public void testPrinter() {

        DataFrame df = DataFrame.fromCSV("users.csv",
                DataFrameLoaderTest.class.getClassLoader(), ';', true);
        df.addColumn(Double.class, "value", ColumnTypeMap.create(), row -> Math.random());
        String corner = "o";
        String hline = "|";
        String vline = "-";
        String joint = "x";
        int width = 10;
        int cwidth = 8;
        Printer printer = PrinterBuilder.create()
                .withColumnHeaderFormatter("name", (h, m) -> "||" + h.toString())
                .withColumnValueFormatter("name", (v, m) -> v.toString().toUpperCase())
                .withHorizontalLine(hline)
                .withVerticalLine(vline)
                .withJoint(joint)
                .withCorner(corner)
                .withDefaultColumnWidth(width)
                .withDefaultMaxContentWidth(cwidth)
                .build();
        StringWriter sw = new StringWriter();
        df.write(sw, printer);
        String[] lines = sw.toString().split("\\r?\\n");

        //content lines (+header) + inner lines + outer lines
        int expected = (df.size() + 1) + df.size() + 2;
        Assert.assertEquals(expected, lines.length);

        //cols * width + inner lines + outer lines
        int cols = df.getRow(0).size();
        int expectedRow = cols*width + cols-1 + 2;

        for (int i = 0; i < lines.length; i++) {
            Assert.assertEquals(expectedRow, lines[i].length());
            //top / bottom line
            if (i == 0 || i == lines.length - 1) {
                Assert.assertEquals(corner, String.valueOf(lines[i].charAt(0)));
                Assert.assertEquals(corner, String.valueOf(lines[i].charAt(lines[i].length() - 1)));
                for(int j = 1; j < expectedRow  -1; j++){
                    if(j % (width + 1) == 0){
                        Assert.assertEquals(joint,String.valueOf(lines[i].charAt(j)));
                    }
                    else{
                        Assert.assertEquals(hline,String.valueOf(lines[i].charAt(j)));
                    }
                }
                continue;
            }


            //Border line
            if (i % 2 == 0) {
                Assert.assertEquals(joint, String.valueOf(lines[i].charAt(0)));
                Assert.assertEquals(joint, String.valueOf(lines[i].charAt(lines[i].length() - 1)));
                for(int j = 1; j < expectedRow  -1; j++){
                    if(j % (width + 1) == 0){
                        Assert.assertEquals(joint,String.valueOf(lines[i].charAt(j)));
                    }
                    else{
                        Assert.assertEquals(hline,String.valueOf(lines[i].charAt(j)));
                    }
                }
                continue;
            }

            //Content line
            //TODO
        }
    }
}
