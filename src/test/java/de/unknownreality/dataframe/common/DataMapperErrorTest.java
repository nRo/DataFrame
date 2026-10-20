package de.unknownreality.dataframe.common;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.common.mapping.DataMapper;
import de.unknownreality.dataframe.common.mapping.MappedColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Mapping errors must be reported as {@link DataFrameRuntimeException}
 */
public class DataMapperErrorTest {

    private static DataFrame frame() {
        DataFrame df = DataFrame.create().addStringColumn("name");
        df.append("A");
        df.append("B");
        return df;
    }

    @Test
    public void testNoDefaultConstructor() {
        DataFrame df = frame();
        PrintStream originalErr = System.err;
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        DataFrameRuntimeException e;
        try {
            e = Assertions.assertThrows(DataFrameRuntimeException.class, () -> DataMapper.map(df, NoDefaultConstructor.class));
        } finally {
            System.setErr(originalErr);
        }
        Assertions.assertTrue(e.getMessage().contains(NoDefaultConstructor.class.getName()), e.getMessage());
        Assertions.assertNotNull(e.getCause());
        Assertions.assertEquals("", err.toString(StandardCharsets.UTF_8), "nothing must be printed to System.err");
    }

    @Test
    public void testNoDefaultConstructorWithIterable() {
        DataFrame df = frame();
        Iterable<NoDefaultConstructor> mapped = DataMapper.mapEach(df, NoDefaultConstructor.class);
        DataFrameRuntimeException e = Assertions.assertThrows(DataFrameRuntimeException.class,
                () -> mapped.iterator().next());
        Assertions.assertTrue(e.getMessage().contains(NoDefaultConstructor.class.getName()), e.getMessage());
    }

    @Test
    public void testAbstractClass() {
        DataFrame df = frame();
        DataFrameRuntimeException e = Assertions.assertThrows(DataFrameRuntimeException.class,
                () -> DataMapper.map(df, AbstractMapped.class));
        Assertions.assertTrue(e.getMessage().contains(AbstractMapped.class.getName()), e.getMessage());
    }

    @Test
    public void testConstructorThrows() {
        DataFrame df = frame();
        DataFrameRuntimeException e = Assertions.assertThrows(DataFrameRuntimeException.class,
                () -> DataMapper.map(df, ThrowingConstructor.class));
        Assertions.assertTrue(e.getMessage().contains(ThrowingConstructor.class.getName()), e.getMessage());
        Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
    }

    @Test
    public void testNonPublicConstructorIsAccepted() {
        DataFrame df = frame();
        Assertions.assertEquals(2, DataMapper.map(df, PrivateConstructor.class).size());
    }

    public static class NoDefaultConstructor {
        @MappedColumn
        private String name;

        public NoDefaultConstructor(String name) {
            this.name = name;
        }
    }

    public abstract static class AbstractMapped {
        @MappedColumn
        private String name;
    }

    public static class ThrowingConstructor {
        @MappedColumn
        private String name;

        public ThrowingConstructor() {
            throw new IllegalStateException("broken");
        }
    }

    public static class PrivateConstructor {
        @MappedColumn
        private String name;

        private PrivateConstructor() {
        }
    }
}
