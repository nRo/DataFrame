package de.unknownreality.dataframe.common;

import java.util.Comparator;

public class ValueComparator implements Comparator<Comparable> {

    public final static ValueComparator COMPARATOR = new ValueComparator(1);
    private int naOrder = 1;

    public ValueComparator(int naOrder){
        this.naOrder = naOrder;
    }

    @Override
    public int compare(Comparable o1, Comparable o2) {
        if(o1 == null && o2 == null){
            return 0;
        }
        if(o1 == null) {
            return -1*naOrder;
        }
        if(o2 == null){
            return 1*naOrder;
        }
        return o1.compareTo(o2);
    }
}
