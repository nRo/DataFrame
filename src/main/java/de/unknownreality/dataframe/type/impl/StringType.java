package de.unknownreality.dataframe.type.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.settings.EncodingSetting;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class StringType extends ComparableType<String> {
    private final static EncodingSetting DEFAULT_ENCODING_SETTING = EncodingSetting.UTF8;

    public StringType(ColumnSettings columnSettings) {
        super(columnSettings);
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }

    private Charset getEncoding() {
        return getColumnSettings().getOrDefault(EncodingSetting.class, DEFAULT_ENCODING_SETTING).getCharset();
    }

    @Override
    public String read(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] data = new byte[length];
        dis.read(data);
        return new String(data, getEncoding());
    }

    @Override
    public String read(ByteBuffer buf) {
        int length = buf.getInt();
        byte[] data = new byte[length];
        return new String(data, getEncoding());
    }

    @Override
    public String convertRaw(Object o) {
        return super.convertRaw(o == null ? null : String.valueOf(o));
    }

    @Override
    public String parse(String s) {
        assertNotNull(s);
        return s;
    }

    @Override
    public void write(Writer writer, String value) throws IOException {
        assertNotNull(value);
        writer.write(value);
    }

    @Override
    public String toString(String value) {
        assertNotNull(value);
        return value;
    }

    @Override
    public int write(DataOutputStream dos, String value) throws IOException {
        assertNotNull(value);
        byte[] data = value.getBytes(getEncoding());
        dos.writeInt(data.length);
        dos.write(data);
        return Integer.BYTES + data.length;
    }
}
