package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.DoubleType;

public class DoubleValueTypeTest extends NumberValueTypeTest<Double, DoubleType> {
    @Override
    public DoubleType getValueType() {
        return new DoubleType();
    }
}
