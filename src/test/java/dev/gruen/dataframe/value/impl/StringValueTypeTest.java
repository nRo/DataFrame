package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.StringType;

public class StringValueTypeTest extends AbstractValueTypeTest<String, StringType> {
    @Override
    public StringType getValueType() {
        return new StringType();
    }
}
