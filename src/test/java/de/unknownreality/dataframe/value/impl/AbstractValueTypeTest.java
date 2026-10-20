package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.type.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.StringWriter;

abstract class AbstractValueTypeTest<V, T extends ValueType<V>> {
    public abstract T getValueType();

    @Test
    public void nullTest() {
        T valueType = getValueType();

        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.parse(null));
        Assertions.assertNull(valueType.parseOrNull(null));

        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.toString(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.toStringRaw(null));

        StringWriter writer = new StringWriter();
        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.write(writer, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.writeRaw(writer, null));

        DataOutputStream dos = new DataOutputStream(new ByteArrayOutputStream());
        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.write(dos, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.writeRaw(dos, null));

        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.convertRaw(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> valueType.compareRaw(null, null));
    }
}
