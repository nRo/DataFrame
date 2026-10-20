package dev.gruen.dataframe.print;

import dev.gruen.dataframe.DataFrame;
import dev.gruen.dataframe.io.FileFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

public class PrintTargetTest {

    private static DataFrame frame() {
        DataFrame df = DataFrame.create()
                .addStringColumn("name")
                .addIntegerColumn("value");
        df.append("A", 1);
        df.append("M\u00fcller", 2);
        return df;
    }

    @Test
    public void testPrintToAppendable() {
        StringBuilder sb = new StringBuilder();
        frame().print(sb);
        String output = sb.toString();
        Assertions.assertTrue(output.contains("name"), output);
        Assertions.assertTrue(output.contains("M\u00fcller"), output);
        Assertions.assertTrue(output.length() > 0);
    }

    @Test
    public void testPrintToWriter() {
        StringWriter writer = new StringWriter();
        frame().print(writer);
        Assertions.assertTrue(writer.toString().contains("M\u00fcller"));
    }

    @Test
    public void testPrintToPrintStream() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PrintStream stream = new PrintStream(out, true, StandardCharsets.UTF_8)) {
            frame().print(stream);
        }
        String output = out.toString(StandardCharsets.UTF_8);
        Assertions.assertTrue(output.contains("M\u00fcller"), output);
    }

    @Test
    public void testAppendableAndPrintStreamProduceSameOutput() {
        StringBuilder sb = new StringBuilder();
        frame().print(sb);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, true, StandardCharsets.UTF_8);
        frame().print(stream);

        Assertions.assertEquals(sb.toString(), out.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrintWithFormat() {
        StringBuilder sb = new StringBuilder();
        dev.gruen.dataframe.DataFrameWriter.print(frame(), FileFormat.CSV, sb);
        String output = sb.toString();
        Assertions.assertTrue(output.startsWith("name;value"), output);
    }
}
