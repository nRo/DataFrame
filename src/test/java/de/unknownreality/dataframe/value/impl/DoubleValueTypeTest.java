package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.impl.DoubleType;

public class DoubleValueTypeTest extends NumberValueTypeTest<Double, DoubleType> {
    @Override
    public DoubleType getValueType() {
        return new DoubleType(new ColumnSettings());
    }
}
