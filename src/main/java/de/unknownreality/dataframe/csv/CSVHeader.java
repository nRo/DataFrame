package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.BasicHeader;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVHeader extends BasicHeader {

    /**
     * Adds a new column without specified name.
     * If no name is provided, names are created: <tt>V1,V2,V3...</tt>
     */
    public void add() {
        String name = "V" + (size() + 1);
        super.add(name);
    }

}
