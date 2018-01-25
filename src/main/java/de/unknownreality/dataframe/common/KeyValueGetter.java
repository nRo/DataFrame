package de.unknownreality.dataframe.common;

public interface KeyValueGetter<K,V> {
    V get(K name);
}
