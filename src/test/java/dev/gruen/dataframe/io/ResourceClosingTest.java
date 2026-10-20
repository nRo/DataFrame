package dev.gruen.dataframe.io;

import dev.gruen.dataframe.DataFrameRuntimeException;
import dev.gruen.dataframe.csv.CSVFormat;
import dev.gruen.dataframe.csv.CSVIterator;
import dev.gruen.dataframe.csv.CSVReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Readers and streams must be closed if creating or consuming an iterator fails
 */
public class ResourceClosingTest {

    private static class TrackingReader extends StringReader {
        final AtomicInteger closed = new AtomicInteger();

        TrackingReader(String s) {
            super(s);
        }

        @Override
        public void close() {
            closed.incrementAndGet();
            super.close();
        }
    }

    /**
     * Delivers some content and fails with an {@link IOException} afterwards
     */
    private static class FailingReader extends Reader {
        private final String content;
        private int pos = 0;
        final AtomicInteger closed = new AtomicInteger();

        FailingReader(String content) {
            this.content = content;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (pos >= content.length()) {
                throw new IOException("simulated read failure");
            }
            int n = Math.min(len, content.length() - pos);
            content.getChars(pos, pos + n, cbuf, off);
            pos += n;
            return n;
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }

    private static class TrackingInputStream extends ByteArrayInputStream {
        final AtomicInteger closed = new AtomicInteger();

        TrackingInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public void close() throws IOException {
            closed.incrementAndGet();
            super.close();
        }
    }

    @Test
    public void testReaderClosedIfHeaderIsMissing() {
        TrackingReader reader = new TrackingReader("");
        CSVReader csvReader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> csvReader.load(reader));
        Assertions.assertTrue(reader.closed.get() >= 1, "reader must be closed if the header can not be read");
    }

    @Test
    public void testReaderClosedIfHeaderReadFails() {
        FailingReader reader = new FailingReader("");
        CSVReader csvReader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> csvReader.load(reader));
        Assertions.assertTrue(reader.closed.get() >= 1, "reader must be closed if the header can not be read");
    }

    @Test
    public void testInputStreamClosedIfIteratorCreationFails() {
        TrackingInputStream stream = new TrackingInputStream(new byte[0]);
        CSVReader csvReader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> csvReader.load(stream));
        Assertions.assertTrue(stream.closed.get() >= 1, "stream must be closed if the iterator can not be created");
    }

    @Test
    public void testInputStreamClosedIfGzipHeaderIsCorrupt() {
        // gzip magic number followed by garbage
        byte[] corrupt = new byte[]{0x1f, (byte) 0x8b, 0x00, 0x01, 0x02};
        TrackingInputStream stream = new TrackingInputStream(corrupt);
        CSVReader csvReader = CSVFormat.createReader().build();
        Assertions.assertThrows(DataFrameRuntimeException.class, () -> csvReader.load(stream));
        Assertions.assertTrue(stream.closed.get() >= 1, "stream must be closed if the gzip header is corrupt");
    }

    @Test
    public void testTryWithResourcesClosesIterator() {
        TrackingReader reader = new TrackingReader("a;b\n1;2\n3;4\n");
        try (CSVIterator iterator = CSVFormat.createReader().build().load(reader)) {
            Assertions.assertTrue(iterator.hasNext());
            iterator.next();
        }
        Assertions.assertTrue(reader.closed.get() >= 1);
    }

    @Test
    public void testIteratorClosesAfterLastRow() {
        TrackingReader reader = new TrackingReader("a;b\n1;2\n");
        CSVIterator iterator = CSVFormat.createReader().build().load(reader);
        while (iterator.hasNext()) {
            iterator.next();
        }
        Assertions.assertTrue(reader.closed.get() >= 1);
    }

    @Test
    public void testSkipPropagatesError() {
        FailingReader reader = new FailingReader("a;b\n1;2\n");
        try (CSVIterator iterator = CSVFormat.createReader().build().load(reader)) {
            DataFrameRuntimeException e = Assertions.assertThrows(DataFrameRuntimeException.class, () -> iterator.skip(5));
            Assertions.assertInstanceOf(IOException.class, e.getCause());
        }
        Assertions.assertTrue(reader.closed.get() >= 1);
    }

    @Test
    public void testStreamConstructorHonorsCharset() throws IOException {
        InputStream stream = new ByteArrayInputStream("a;b\n\u00fc;2\n".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
        CSVReader csvReader = CSVFormat.createReader().withCharset(java.nio.charset.StandardCharsets.ISO_8859_1).build();
        try (CSVIterator iterator = csvReader.load(stream)) {
            Assertions.assertEquals("\u00fc", iterator.next().get(0));
        }
    }
}
