package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.BooleanType;

public class BooleanValueTypeTest extends AbstractValueTypeTest<Boolean, BooleanType> {
    @Override
    public BooleanType getValueType() {
        return new BooleanType();
    }
}
