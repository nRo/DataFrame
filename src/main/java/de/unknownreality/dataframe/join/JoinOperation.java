package de.unknownreality.dataframe.join;

import de.unknownreality.dataframe.DataFrame;

public interface JoinOperation {
    JoinedDataFrame join(DataFrame dfA, DataFrame dfB, String joinSuffixA, String joinSuffixB, JoinColumn... joinColumns);
}
