package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.impl.BooleanType;

public class BooleanValueTypeTest extends AbstractValueTypeTest<Boolean, BooleanType> {
    @Override
    public BooleanType getValueType() {
        return new BooleanType(new ColumnSettings());
    }
}
