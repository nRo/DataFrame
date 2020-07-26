package de.unknownreality.dataframe.value;

import de.unknownreality.dataframe.column.BasicColumn;
import de.unknownreality.dataframe.type.ValueType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomColumn extends BasicColumn<CustomColumn.Custom, CustomColumn> {
    private CustomValueType type = new CustomValueType();

    public CustomColumn() {
        super(Custom.class);
    }

    public CustomColumn(String name) {
        super(name, Custom.class);
    }

    public CustomColumn(String name, Custom[] values) {
        super(name, values, values.length);
    }

    @Override
    protected CustomColumn getThis() {
        return this;
    }

    @Override
    public ValueType<Custom> getValueType() {
        return type;
    }

    @Override
    public CustomColumn copy() {
        return new CustomColumn(getName(), values);
    }

    @Override
    public CustomColumn copyEmpty() {
        return new CustomColumn(getName());
    }

    public static class Custom {
        public int x;
        public int y;

        public Custom(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Custom custom = (Custom) o;
            return x == custom.x &&
                    y == custom.y;
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }
    }

    public static class CustomValueType extends ValueType<Custom> {
        private Pattern PATTERN = Pattern.compile("\\[([0-9+]+),\\s*([0-9+]+)]");

        @Override
        public Class<Custom> getType() {
            return Custom.class;
        }

        @Override
        public Comparator<Custom> getComparator() {
            return Comparator.comparingInt((Custom o) -> o.x).thenComparingInt(o -> o.y);
        }

        @Override
        public Custom read(DataInputStream dis) throws IOException {
            return new Custom(dis.readInt(), dis.readInt());
        }

        @Override
        public Custom read(ByteBuffer buf) {
            return new Custom(buf.getInt(), buf.getInt());
        }

        @Override
        public Custom parse(String s) throws ParseException {
            Matcher m = PATTERN.matcher(s);
            if (m.matches()) {
                return new Custom(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
            }
            throw new ParseException(String.format("error parsing %s: no match found"), 0);
        }

        @Override
        public void write(Writer writer, Custom value) throws IOException {
            writer.write(toString(value));
        }

        @Override
        public String toString(Custom value) {
            return String.format("[%d,%d]", value.x, value.y);
        }

        @Override
        public int write(DataOutputStream dos, Custom value) throws IOException {
            dos.writeInt(value.x);
            dos.writeInt(value.y);
            return 2 * Integer.BYTES;
        }
    }
}
