package de.unknownreality.dataframe.group;

import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.DefaultDataFrameHeader;

/**
 * Created by algru on 11.06.2017.
 */
public class GroupRow extends DataRow {
    public GroupRow(DefaultDataFrameHeader header, Comparable[] values, int index) {
        super(header, values, index);
    }
}
