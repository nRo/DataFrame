package de.unknownreality.dataframe.frame.join;

import de.unknownreality.dataframe.frame.DataFrame;
import de.unknownreality.dataframe.frame.DataFrameHeader;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 13.03.2016.
 */
public class JoinInfo {
    private DataFrameHeader header;
    private WeakReference<DataFrame> dataFrameA;
    private WeakReference<DataFrame> dataFrameB;

    public JoinInfo(DataFrameHeader header,DataFrame dataFrameA,DataFrame dataFrameB){
        this.header = header;
        this.dataFrameA = new WeakReference<>(dataFrameA);
        this.dataFrameB = new WeakReference<>(dataFrameB);
    }

    public boolean isA(DataFrame dataFrame){
        DataFrame df;
        return ((df = dataFrameA.get()) != null && df == dataFrame);
    }
    public boolean isB(DataFrame dataFrame){
        DataFrame df;
        return ((df = dataFrameB.get()) != null && df == dataFrame);
    }


    private Map<String,String> dataFrameAHeaderMap = new HashMap<>();
    private Map<String,String> dataFrameBHeaderMap = new HashMap<>();

    public void addDataFrameAHeader(String original,String joined){
        dataFrameAHeaderMap.put(original,joined);
    }
    public void addDataFrameBHeader(String original,String joined){
        dataFrameBHeaderMap.put(original,joined);
    }

    public String getJoinedHeader(String original, DataFrame dataFrame){
        if(isA(dataFrame)){
            return getJoinedHeaderA(original);
        }
        else{
            return getJoinedHeaderB(original);
        }
    }

    public int getJoinedIndex(String original, DataFrame dataFrame){
        if(isA(dataFrame)){
            return getJoinedIndexA(original);
        }
        else{
            return getJoinedIndexB(original);
        }
    }

    public String getJoinedHeaderA(String original){
        return dataFrameAHeaderMap.get(original);
    }

    public int getJoinedIndexA(String original){
        return this.header.getIndex(getJoinedHeaderA(original));
    }
    public int getJoinedIndexB(String original){
        return this.header.getIndex(getJoinedHeaderB(original));
    }
    public String getJoinedHeaderB(String original){
        return dataFrameBHeaderMap.get(original);
    }
}
