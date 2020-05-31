package de.unknownreality.dataframe.type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Comparator;

public abstract class ValueType<T> {
    public abstract Class<T> getType();

    public abstract Comparator<T> getComparator();

    public abstract T read(DataInputStream dis) throws IOException;

    public abstract T read(ByteBuffer buf);

    public abstract T parse(String s) throws ParseException;

    public T parseOrNull(String s) {
        try {
            return parse(s);
        } catch (Exception e) {
            return null;
        }
    }

    public abstract void write(Writer writer, T value) throws IOException;

    public abstract String toString(T value);

    public abstract int write(DataOutputStream dos, T value) throws IOException;


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(getClass().isAssignableFrom(obj.getClass()))) {
            return false;
        }
        ValueType<?> other = (ValueType<?>) obj;
        return getType().equals(other.getType());
    }

    @Override
    public int hashCode() {
        return getType().hashCode();
    }
}
