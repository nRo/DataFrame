package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.ByteType;

public class ByteValueTypeTest extends NumberValueTypeTest<Byte, ByteType> {
    @Override
    public ByteType getValueType() {
        return new ByteType();
    }
}
