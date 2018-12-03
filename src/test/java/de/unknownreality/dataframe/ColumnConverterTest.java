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

package de.unknownreality.dataframe;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameException;
import de.unknownreality.dataframe.column.DoubleColumn;
import de.unknownreality.dataframe.column.IntegerColumn;
import de.unknownreality.dataframe.transform.StringColumnConverter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Alex on 28.06.2017.
 */
public class ColumnConverterTest {
    @Test
    public void testColumnConverter() throws DataFrameException {
        DataFrame dataFrame = DataFrame.create();
        dataFrame.addStringColumn("double");
        dataFrame.addStringColumn("integer");
        dataFrame.append("1","1");
        dataFrame.append("2","2");
        dataFrame.append("3","3");
        dataFrame.append("3.5","4");
        dataFrame.append("4.5","5");


        DoubleColumn doubles = StringColumnConverter.convert(dataFrame.getStringColumn("double"), DoubleColumn.class);
        IntegerColumn integers = StringColumnConverter.convert(dataFrame.getStringColumn("integer"),IntegerColumn.class);

        for(int i = 0; i < dataFrame.size(); i++){
            Assert.assertEquals((Double)Double.parseDouble(dataFrame.getRow(i).getString("double")), doubles.get(i));
            Assert.assertEquals((Integer) Integer.parseInt(dataFrame.getRow(i).getString("integer")), integers.get(i));
        }
    }
}
