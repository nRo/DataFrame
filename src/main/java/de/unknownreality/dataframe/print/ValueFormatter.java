package de.unknownreality.dataframe.print;

public interface ValueFormatter<T> {
    String format(T value);
}
