package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.type.impl.StringType;

public class StringValueTypeTest extends AbstractValueTypeTest<String, StringType> {
    @Override
    public StringType getValueType() {
        return new StringType();
    }
}
