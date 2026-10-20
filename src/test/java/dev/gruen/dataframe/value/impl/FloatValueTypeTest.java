package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.FloatType;

public class FloatValueTypeTest extends NumberValueTypeTest<Float, FloatType> {
    @Override
    public FloatType getValueType() {
        return new FloatType();
    }
}
