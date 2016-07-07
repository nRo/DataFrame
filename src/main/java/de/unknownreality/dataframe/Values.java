package de.unknownreality.dataframe;

/**
 * Created by Alex on 12.03.2016.
 */
public class Values {
    public static final NA NA = new NA();


    public static class NA implements Comparable {

        public boolean isNA(String s) {
            return toString().equals(s);
        }

        private NA() {

        }

        @Override
        public int compareTo(Object o) {
            if (o == this) {
                return 0;
            }
            return -1;
        }

        public String toString() {
            return "NA";
        }
    }
}
