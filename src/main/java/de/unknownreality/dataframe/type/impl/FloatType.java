package de.unknownreality.dataframe.type.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;

public class FloatType extends ComparableType<Float> {
    @Override
    public Class<Float> getType() {
        return Float.class;
    }


    @Override
    public Float read(DataInputStream dis) throws IOException {
        return dis.readFloat();
    }

    @Override
    public Float read(ByteBuffer buf) {
        return buf.getFloat();
    }

    @Override
    public Float parse(String s) {
        return Float.parseFloat(s);
    }

    @Override
    public void write(Writer writer, Float value) throws IOException {
        writer.write(toString(value));
    }

    @Override
    public String toString(Float value) {
        return Float.toString(value);
    }


    @Override
    public int write(DataOutputStream dos, Float value) throws IOException {
        dos.writeFloat(value);
        return Float.BYTES;
    }
}
