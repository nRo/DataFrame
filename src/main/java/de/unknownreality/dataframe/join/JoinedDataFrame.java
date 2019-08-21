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

package de.unknownreality.dataframe.join;

import de.unknownreality.dataframe.DefaultDataFrame;

/**
 * Created by Alex on 13.03.2016.
 */
public class JoinedDataFrame extends DefaultDataFrame {
    private final JoinInfo joinInfo;

    public JoinedDataFrame(JoinInfo info) {
        this.joinInfo = info;
    }

    /**
     * Returns the information about the joined columns
     *
     * @return join info
     */
    public JoinInfo getJoinInfo() {
        return joinInfo;
    }


    @Override
    public JoinedDataFrame copy() {
        JoinedDataFrame joinedDataFrame = new JoinedDataFrame(joinInfo);
        joinedDataFrame.set(getRows(),getIndices());
        return joinedDataFrame;
    }

}
