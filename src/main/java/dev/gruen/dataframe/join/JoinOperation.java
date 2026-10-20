package dev.gruen.dataframe.join;

import dev.gruen.dataframe.DataFrame;

public interface JoinOperation {
    JoinedDataFrame join(DataFrame dfA, DataFrame dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns);
}
