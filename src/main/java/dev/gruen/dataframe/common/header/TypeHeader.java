package dev.gruen.dataframe.common.header;

import dev.gruen.dataframe.type.ValueType;

public interface TypeHeader<T> extends Header<T> {
    ValueType<?> getValueType(int index);

    ValueType<?> getValueType(T name);
}
