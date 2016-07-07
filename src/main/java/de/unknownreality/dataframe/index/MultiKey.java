package de.unknownreality.dataframe.index;

import java.util.Arrays;

/**
 * Created by Alex on 27.05.2016.
 */
public class MultiKey {
    private final Comparable[] values;
    private int hash;

    public MultiKey(Comparable[] values) {
        this.values = values;
        updateHash();
    }

    public void update(int index, Comparable value) {
        values[index] = value;
        updateHash();
    }


    public void updateHash() {
        hash = Arrays.asList(values).hashCode();
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MultiKey)) {
            return false;
        }
        MultiKey other = (MultiKey) o;
        return hashCode() == other.hashCode() && Arrays.equals(values, other.values);
    }

    public Object[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
