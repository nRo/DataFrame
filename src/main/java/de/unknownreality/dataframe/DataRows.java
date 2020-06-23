/*
 *
 *  * Copyright (c) 2019 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe;

import java.util.ArrayList;
import java.util.List;

public class DataRows extends ArrayList<DataRow>{

    private final DataFrame dataFrame;

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
