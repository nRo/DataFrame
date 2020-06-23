package de.unknownreality.dataframe.value;

import de.unknownreality.dataframe.column.BasicColumn;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.type.ValueType;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomValueTypeTest {

    @Test
    public void createCustomValueTypeColumn() {

    }

    private class CustomColumn extends BasicColumn<Custom, CustomColumn> {
        private CustomValueType type = new CustomValueType();

        public CustomColumn() {
            super(Custom.class);
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
            return new CustomColumn();
        }

        @Override
        public <H> Custom getValueFromRow(Row<?, H> row, H headerName) {
            return row.get(headerName, Custom.class);
        }

        @Override
        public Custom getValueFromRow(Row<?, ?> row, int headerIndex) {
            return null;
        }
    }


    private class Custom {
        public int x;
        public int y;

        public Custom(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    private class CustomValueType extends ValueType<Custom> {
        private Pattern PATTERN = Pattern.compile("\\[([0-9+]+),\\s*([0-9+]+)\\]");

        @Override
        public Class<Custom> getType() {
            return Custom.class;
        }

        @Override
        public Comparator<Custom> getComparator() {
            return (o1, o2) -> {
                int c = Integer.compare(o1.x, o2.y);
                if (c == 0) {
                    c = Integer.compare(o1.y, o2.y);
                }
                return c;
            };
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
