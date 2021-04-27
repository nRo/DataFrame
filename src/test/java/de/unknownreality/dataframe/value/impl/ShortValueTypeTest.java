package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.impl.ShortType;

public class ShortValueTypeTest extends AbstractValueTypeTest<Short, ShortType> {
    @Override
    public ShortType getValueType() {
        return new ShortType(new ColumnSettings());
    }
}
