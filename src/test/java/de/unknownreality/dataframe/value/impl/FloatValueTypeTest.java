package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.impl.FloatType;

public class FloatValueTypeTest extends NumberValueTypeTest<Float, FloatType> {
    @Override
    public FloatType getValueType() {
        return new FloatType(new ColumnSettings());
    }
}
