package dev.gruen.dataframe.io;

import dev.gruen.dataframe.DataFrame;
import dev.gruen.dataframe.DataFrameConverter;
import dev.gruen.dataframe.DataFrameRuntimeException;
import dev.gruen.dataframe.csv.CSVFormat;
import dev.gruen.dataframe.csv.CSVReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Charset handling must not depend on the platform default charset.
 * This test class is executed a second time with <code>-Dfile.encoding=ISO-8859-1</code> (see pom.xml).
 */
public class DataReaderCharsetTest {
    private static final String MUELLER = "M\u00fcller";
    private static final String TOKYO = "\u6771\u4eac";
    private static final String ZUERICH = "Z\u00fcrich";

    @TempDir
    File tempDir;

    private static DataFrame unicodeFrame() {
        DataFrame df = DataFrame.create()
                .addStringColumn("name")
                .addStringColumn("city");
        df.append(MUELLER, TOKYO);
        df.append("\u00c5ngstr\u00f6m", ZUERICH);
        return df;
    }

    private static DataFrame latin1Frame() {
        DataFrame df = DataFrame.create()
                .addStringColumn("name")
                .addStringColumn("city");
        df.append(MUELLER, ZUERICH);
        df.append("\u00c5ngstr\u00f6m", "Malm\u00f6");
        return df;
    }

    private static void assertSameContent(DataFrame expected, DataFrame actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertEquals(expected.getColumns().size(), actual.getColumns().size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.getRow(i).getString("name"), actual.getRow(i).getString("name"));
            Assertions.assertEquals(expected.getRow(i).getString("city"), actual.getRow(i).getString("city"));
        }
    }

    private static DataFrame read(DataIterator<?> iterator) {
        return DataFrameConverter.fromDataIterator(iterator);
    }

    private static byte[] gunzip(byte[] bytes) throws IOException {
        try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            return is.readAllBytes();
        }
    }

    @Test
    public void testDefaultCharsetIsUtf8() {
        Assertions.assertEquals(StandardCharsets.UTF_8, CSVFormat.createReader().build().getCharset());
        Assertions.assertEquals(StandardCharsets.UTF_8, CSVFormat.createWriter().build().getCharset());
    }

    @Test
    public void testPlainRoundTrip() throws IOException {
        File file = new File(tempDir, "plain.csv");
        DataFrame df = unicodeFrame();
        CSVFormat.createWriter().build().write(file, df);

        String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        Assertions.assertTrue(content.contains(MUELLER));
        Assertions.assertTrue(content.contains(TOKYO));

        assertSameContent(df, read(CSVFormat.createReader().build().load(file)));
    }

    @Test
    public void testGzipFileRoundTrip() throws IOException {
        File file = new File(tempDir, "gzip.csv.gz");
        DataFrame df = unicodeFrame();
        CSVFormat.createWriter().useGzip(true).build().write(file, df);

        String content = new String(gunzip(Files.readAllBytes(file.toPath())), StandardCharsets.UTF_8);
        Assertions.assertTrue(content.contains(MUELLER));
        Assertions.assertTrue(content.contains(TOKYO));

        assertSameContent(df, read(CSVFormat.createReader().build().load(file)));
    }

    @Test
    public void testStreamRoundTrip() throws IOException {
        DataFrame df = unicodeFrame();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CSVFormat.createWriter().build().write(out, df);
        byte[] bytes = out.toByteArray();

        CSVReader reader = CSVFormat.createReader().build();
        assertSameContent(df, read(reader.load(new ByteArrayInputStream(bytes))));
        assertSameContent(df, read(reader.load(bytes)));
    }

    @Test
    public void testGzipStreamRoundTrip() {
        DataFrame df = unicodeFrame();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CSVFormat.createWriter().useGzip(true).build().write(out, df);
        byte[] bytes = out.toByteArray();

        // gzip must be detected automatically on every binary path
        CSVReader reader = CSVFormat.createReader().build();
        assertSameContent(df, read(reader.load(new ByteArrayInputStream(bytes))));
        assertSameContent(df, read(reader.load(bytes)));
    }

    @Test
    public void testExplicitCharsetIsHonoredOnAllPaths() throws IOException {
        Charset latin1 = StandardCharsets.ISO_8859_1;
        DataFrame df = latin1Frame();

        File plain = new File(tempDir, "latin1.csv");
        CSVFormat.createWriter().withCharset(latin1).build().write(plain, df);
        byte[] plainBytes = Files.readAllBytes(plain.toPath());
        // 'ü' is a single byte in ISO-8859-1
        Assertions.assertEquals(new String(plainBytes, latin1).length(), plainBytes.length);
        Assertions.assertTrue(new String(plainBytes, latin1).contains(MUELLER));

        File gzip = new File(tempDir, "latin1.csv.gz");
        CSVFormat.createWriter().withCharset(latin1).useGzip(true).build().write(gzip, df);
        Assertions.assertTrue(new String(gunzip(Files.readAllBytes(gzip.toPath())), latin1).contains(MUELLER));

        CSVReader reader = CSVFormat.createReader().withCharset(latin1).build();
        assertSameContent(df, read(reader.load(plain)));
        assertSameContent(df, read(reader.load(gzip)));
        assertSameContent(df, read(reader.load(new ByteArrayInputStream(plainBytes))));
        assertSameContent(df, read(reader.load(plainBytes)));
        assertSameContent(df, read(reader.load(Files.readAllBytes(gzip.toPath()))));
    }

    @Test
    public void testResourceUsesCharset() {
        CSVReader reader = CSVFormat.createReader().build();
        DataFrame df = read(reader.load("charset_utf8.csv", DataReaderCharsetTest.class.getClassLoader()));
        Assertions.assertEquals(2, df.size());
        Assertions.assertEquals(MUELLER, df.getRow(0).getString("name"));
        Assertions.assertEquals(TOKYO, df.getRow(0).getString("city"));
    }

    @Test
    public void testUtf8Bom() {
        byte[] content = "\uFEFFname;city\nM\u00fcller;Z\u00fcrich\n".getBytes(StandardCharsets.UTF_8);
        DataFrame df = read(CSVFormat.createReader().build().load(content));
        Assertions.assertEquals("name", df.getHeader().get(0));
        Assertions.assertEquals(MUELLER, df.getRow(0).getString("name"));
    }

    @Test
    public void testMissingResource() {
        CSVReader reader = CSVFormat.createReader().build();
        DataFrameRuntimeException e = Assertions.assertThrows(DataFrameRuntimeException.class,
                () -> reader.load("does/not/exist.csv", DataReaderCharsetTest.class.getClassLoader()));
        Assertions.assertTrue(e.getMessage().contains("does/not/exist.csv"), e.getMessage());
    }

    @Test
    public void testMissingFile() {
        CSVReader reader = CSVFormat.createReader().build();
        File missing = new File(tempDir, "missing.csv");
        DataFrameRuntimeException e = Assertions.assertThrows(DataFrameRuntimeException.class,
                () -> reader.load(missing));
        Assertions.assertTrue(e.getMessage().contains("missing.csv"), e.getMessage());
    }

    @Test
    public void testNullStream() {
        CSVReader reader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> reader.load((InputStream) null));
    }

    @Test
    public void testEmptyFileFailsCleanly() throws IOException {
        File file = new File(tempDir, "empty.csv");
        Assertions.assertTrue(file.createNewFile());
        CSVReader reader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> reader.load(file));
    }

    @Test
    public void testHeaderOnlyFile() throws IOException {
        File file = new File(tempDir, "header.csv");
        Files.write(file.toPath(), "name;city\n".getBytes(StandardCharsets.UTF_8));
        DataFrame df = read(CSVFormat.createReader().build().load(file));
        Assertions.assertEquals(0, df.size());
        Assertions.assertEquals(2, df.getColumns().size());
    }

    @Test
    public void testTruncatedGzipFailsCleanly() {
        DataFrame df = DataFrame.create().addStringColumn("name").addIntegerColumn("value");
        for (int i = 0; i < 5000; i++) {
            df.append("name" + i, i);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CSVFormat.createWriter().useGzip(true).build().write(out, df);
        byte[] bytes = out.toByteArray();
        byte[] truncated = Arrays.copyOf(bytes, bytes.length / 2);

        CSVReader reader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> read(reader.load(truncated)));
    }
}
