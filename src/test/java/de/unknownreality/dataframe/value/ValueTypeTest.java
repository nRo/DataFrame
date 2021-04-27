package de.unknownreality.dataframe.value;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;

public class ValueTypeTest {
    String expectedCSVNull =
            "byte\tboolean\tdouble\tfloat\tinteger\tlong\tshort\tstring" + System.lineSeparator() +
                    "1\ttrue\t2.0\t3.0\t4\t5\t6\ttest" + System.lineSeparator() +
                    "NA\tNA\tNA\tNA\tNA\tNA\tNA\tNA" + System.lineSeparator() +
                    "2\tfalse\t2.1\t3.1\t7\t8\t9\ttest2" + System.lineSeparator();

    @Test
    public void testNullValues() {
        DataFrame df = DataFrame.create()
                .addByteColumn("byte")
                .addBooleanColumn("boolean")
                .addDoubleColumn("double")
                .addFloatColumn("float")
                .addIntegerColumn("integer")
                .addLongColumn("long")
                .addShortColumn("short")
                .addStringColumn("string");

        df.append(1, true, 2d, 3f, 4, 5L, 6, "test");
        df.append(null, null, null, null, null, null, null, null);
        df.append(2, false, 2.1d, 3.1f, 7, 8L, 9, "test2");


        StringWriter stringWriter = new StringWriter();
        df.writeCSV(stringWriter, '\t', true);
        String csv = stringWriter.toString();
        Assert.assertEquals(expectedCSVNull, csv);

        CSVReader reader = CSVReaderBuilder.create()
                .setColumnType("byte", Byte.class)
                .setColumnType("boolean", Boolean.class)
                .setColumnType("double", Double.class)
                .setColumnType("float", Float.class)
                .setColumnType("integer", Integer.class)
                .setColumnType("long", Long.class)
                .setColumnType("short", Short.class)
                .setColumnType("string", String.class)
                .withSeparator('\t')
                .withHeader(true)
                .build();

        DataFrame parsedDF = DataFrame.load(csv, reader);
        Assert.assertEquals(df, parsedDF);
    }
}
