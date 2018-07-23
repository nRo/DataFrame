package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.ColumnTypeMap;
import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.print.Printer;
import de.unknownreality.dataframe.print.PrinterBuilder;
import org.junit.Test;

public class PrintTest {
    @Test
    public void testPrinter(){

        DataFrame df = DataFrame.fromCSV("users.csv",
                DataFrameLoaderTest.class.getClassLoader(), ';', true);
        df.addColumn(Double.class, "value", ColumnTypeMap.create(), row -> Math.random());
        Printer printer = PrinterBuilder.create()
                .withColumnHeaderFormatter("name", (h,m) -> "||"+h.toString())
                .withColumnValueFormatter("name", (v,m) -> v.toString().toUpperCase())
                .build();
        df.write(System.out,printer);

    }
}
