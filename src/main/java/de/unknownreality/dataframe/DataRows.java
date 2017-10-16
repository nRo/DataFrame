package de.unknownreality.dataframe;

import java.util.ArrayList;
import java.util.List;

public class DataRows extends ArrayList<DataRow>{

    private DataFrame dataFrame;

    public DataRows(DataFrame dataFrame, List<DataRow> rows){
        this.dataFrame = dataFrame;
        addAll(rows);
    }

    public DataRows(DataFrame dataFrame){
        this.dataFrame = dataFrame;
    }


    public DataFrame toDataFrame(){
        DataFrame df = DataFrame.create();
        df.set(dataFrame.getHeader().copy());
        for(DataRow row : this){
            df.append(row);
        }
        return df;
    }

}
