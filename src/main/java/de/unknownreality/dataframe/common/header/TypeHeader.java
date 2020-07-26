package de.unknownreality.dataframe.common.header;

import de.unknownreality.dataframe.type.ValueType;

public interface TypeHeader<T> extends Header<T> {
    ValueType<?> getValueType(int index);

    ValueType<?> getValueType(T name);
}
