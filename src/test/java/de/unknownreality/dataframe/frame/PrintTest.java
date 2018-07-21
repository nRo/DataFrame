package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.ColumnAppender;
import de.unknownreality.dataframe.ColumnTypeMap;
import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.print.Printer;
import org.junit.Test;

public class PrintTest {
    @Test
    public void testPrinter(){

        DataFrame df = DataFrame.fromCSV("users.csv",
                DataFrameLoaderTest.class.getClassLoader(), ';', true);
        df.addColumn(Double.class, "value", ColumnTypeMap.create(), new ColumnAppender<Double>() {
            @Override
            public Double createRowValue(DataRow row) {
                return Math.random();
            }
        });
        Printer printer = new Printer();
        printer.write(System.out,df);
    }
}
