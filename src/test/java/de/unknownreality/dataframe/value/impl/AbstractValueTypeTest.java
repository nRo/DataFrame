package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.type.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.StringWriter;

abstract class AbstractValueTypeTest<V, T extends ValueType<V>> {
    public abstract T getValueType();

    @Test
    public void nullTest() {
        T valueType = getValueType();

        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.parse(null));
        Assert.assertNull(valueType.parseOrNull(null));

        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.toString(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.toStringRaw(null));

        StringWriter writer = new StringWriter();
        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.write(writer, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.writeRaw(writer, null));

        DataOutputStream dos = new DataOutputStream(new ByteArrayOutputStream());
        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.write(dos, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.writeRaw(dos, null));

        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.convertRaw(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> valueType.compareRaw(null, null));
    }
}
