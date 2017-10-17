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
        if(isEmpty()){
            return df;
        }
        boolean compatible = df.isCompatible(
                get(0).getDataFrame()
        );
        for(DataRow row : this){
            if(compatible){
                df.appendMatchingRow(row);
            }
            else{
                df.append(row);
            }

        }
        return df;
    }

}
