package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.LongType;

public class LongValueTypeTest extends AbstractValueTypeTest<Long, LongType> {
    @Override
    public LongType getValueType() {
        return new LongType();
    }
}
