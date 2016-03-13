package de.unknownreality.data.frame.join;

import de.unknownreality.data.frame.DataFrame;

/**
 * Created by Alex on 13.03.2016.
 */
public class JoinedDataFrame extends DataFrame {
   private JoinInfo joinInfo;

    public JoinedDataFrame(JoinInfo info){
        this.joinInfo = info;
    }

    public JoinInfo getJoinInfo() {
        return joinInfo;
    }
}
