package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.impl.LongType;

public class LongValueTypeTest extends AbstractValueTypeTest<Long, LongType> {
    @Override
    public LongType getValueType() {
        return new LongType(new ColumnSettings());
    }
}
