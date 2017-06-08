/*
 *
 *  * Copyright (c) 2017 Alexander Grün
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

package de.unknownreality.dataframe.join;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameHeader;
import de.unknownreality.dataframe.DataFrameRuntimeException;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Alex on 13.03.2016.
 */
public class JoinInfo {
    private final DataFrameHeader header;
    private final WeakReference<DataFrame> dataFrameA;
    private final WeakReference<DataFrame> dataFrameB;

    /**
     * Creates a join information using both data frames and the joined data frame header.<br>
     * The data frames are saved as {@link WeakReference}.
     *
     * @param header     joined data frame header
     * @param dataFrameA first data frame
     * @param dataFrameB second data frame
     */
    public JoinInfo(DataFrameHeader header, DataFrame dataFrameA, DataFrame dataFrameB) {
        this.header = header;
        this.dataFrameA = new WeakReference<>(dataFrameA);
        this.dataFrameB = new WeakReference<>(dataFrameB);
    }

    /**
     * Returns <tt>true</tt> if the specified data frame equals the first data frames used for the join.
     *
     * @param dataFrame input data frame
     * @return <tt>true</tt> if the data frame equals the first data frame
     */
    public boolean isA(DataFrame dataFrame) {
        DataFrame df;
        return ((df = dataFrameA.get()) != null && df.equals(dataFrame));
    }

    /**
     * Returns <tt>true</tt> if the specified data frame equals the second data frames used for the join.
     *
     * @param dataFrame input data frame
     * @return <tt>true</tt> if the data frame equals the second data frame
     */
    public boolean isB(DataFrame dataFrame) {
        DataFrame df;
        return ((df = dataFrameB.get()) != null && df.equals(dataFrame));
    }


    private final Map<String, String> dataFrameAHeaderMap = new HashMap<>();
    private final Map<String, String> dataFrameBHeaderMap = new HashMap<>();

    /**
     * Adds a original - joined header information for the first data frame.
     *
     * @param original header name in the first data frame
     * @param joined   header name in the joined data frame
     */
    protected void addDataFrameAHeader(String original, String joined) {
        dataFrameAHeaderMap.put(original, joined);
    }

    /**
     * Adds a original - joined header information for the second data frame.
     *
     * @param original header name in the second data frame
     * @param joined   header name in the joined data frame
     */
    protected void addDataFrameBHeader(String original, String joined) {
        dataFrameBHeaderMap.put(original, joined);
    }

    /**
     * Returns the header name in the joined data frame for a header name from one of the original data frames
     * This method throws a {@link DataFrameRuntimeException} if the data frame was not used for the join.
     *
     * @param original  original header name
     * @param dataFrame data frame used for the join
     * @return header name in the joined data frame
     */
    public String getJoinedHeader(String original, DataFrame dataFrame) {
        if (isA(dataFrame)) {
            return getJoinedHeaderA(original);
        } else if (isB(dataFrame)) {
            return getJoinedHeaderB(original);
        }
        throw new DataFrameRuntimeException("the provided data frame was not used for the join");
    }

    /**
     * Returns the column index in the joined data frame for a header name from one of the original data frames
     * This method throws a {@link DataFrameRuntimeException} if the data frame was not used for the join.
     *
     * @param original  original header name
     * @param dataFrame data frame used for the join
     * @return header name in the joined data frame
     */
    public int getJoinedIndex(String original, DataFrame dataFrame) {
        if (isA(dataFrame)) {
            return getJoinedIndexA(original);
        } else if (isB(dataFrame)) {
            return getJoinedIndexB(original);
        }
        throw new DataFrameRuntimeException("the provided data frame was not used for the join");
    }

    /**
     * Returns the header name in the joined data frame for a header name from the first data frame
     *
     * @param original header name in the first data frame
     * @return header name in the joined data frame
     */
    public String getJoinedHeaderA(String original) {
        return dataFrameAHeaderMap.get(original);
    }

    /**
     * Returns the column index in the joined data frame for a header name from the first data frame
     *
     * @param original header name in the first data frame
     * @return column index in the joined data frame
     */
    public int getJoinedIndexA(String original) {
        return this.header.getIndex(getJoinedHeaderA(original));
    }

    /**
     * Returns the column index in the joined data frame for a header name from the second data frame
     *
     * @param original header name in the second data frame
     * @return column index in the joined data frame
     */
    public int getJoinedIndexB(String original) {
        return this.header.getIndex(getJoinedHeaderB(original));
    }

    /**
     * Returns the header name in the joined data frame for a header name from the second data frame
     *
     * @param original header name in the second data frame
     * @return header name in the joined data frame
     */
    public String getJoinedHeaderB(String original) {
        return dataFrameBHeaderMap.get(original);
    }
}
