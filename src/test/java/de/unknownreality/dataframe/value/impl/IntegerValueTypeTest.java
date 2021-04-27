package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.type.impl.IntegerType;

public class IntegerValueTypeTest extends NumberValueTypeTest<Integer, IntegerType> {
    @Override
    public IntegerType getValueType() {
        return new IntegerType();
    }
}
