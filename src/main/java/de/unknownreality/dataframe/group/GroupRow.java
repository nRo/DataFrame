package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DataRow;

/**
 * Created by algru on 11.06.2017.
 */
public class GroupRow extends DataRow {
    private DataGroup group;
    public GroupRow(DataGroup group, DataFrameHeader header, Comparable[] values, int index) {
        super(header, values, index);
        this.group = group;
    }

    public DataGroup getGroup() {
        return group;
    }

}
