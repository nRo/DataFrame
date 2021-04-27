package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.settings.ColumnSettings;
import de.unknownreality.dataframe.type.impl.ByteType;

public class ByteValueTypeTest extends NumberValueTypeTest<Byte, ByteType> {
    @Override
    public ByteType getValueType() {
        return new ByteType(new ColumnSettings());
    }
}
