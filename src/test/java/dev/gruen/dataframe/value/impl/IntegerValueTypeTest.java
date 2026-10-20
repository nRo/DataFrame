package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.IntegerType;

public class IntegerValueTypeTest extends NumberValueTypeTest<Integer, IntegerType> {
    @Override
    public IntegerType getValueType() {
        return new IntegerType();
    }
}
