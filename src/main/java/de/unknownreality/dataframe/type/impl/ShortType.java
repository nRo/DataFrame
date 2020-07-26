package de.unknownreality.dataframe.type.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;

public class ShortType extends NumberType<Short> {
    @Override
    public Class<Short> getType() {
        return Short.class;
    }

    @Override
    public Short read(DataInputStream dis) throws IOException {
        return dis.readShort();
    }

    @Override
    public Short read(ByteBuffer buf) {
        return buf.getShort();
    }

    @Override
    public Short parse(String s) {
        return Short.parseShort(s);
    }

    @Override
    public void write(Writer writer, Short value) throws IOException {
        writer.write(toString(value));
    }

    @Override
    public String toString(Short value) {
        return Short.toString(value);
    }


    @Override
    public int write(DataOutputStream dos, Short value) throws IOException {
        dos.writeShort(value);
        return Short.BYTES;
    }

}
