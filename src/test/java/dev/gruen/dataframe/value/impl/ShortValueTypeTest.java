package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.ShortType;

public class ShortValueTypeTest extends AbstractValueTypeTest<Short, ShortType> {
    @Override
    public ShortType getValueType() {
        return new ShortType();
    }
}
