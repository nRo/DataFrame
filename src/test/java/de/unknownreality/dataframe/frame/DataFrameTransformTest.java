/*
 * Copyright (c) 2016 Alexander Grün
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameLoader;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.io.FileFormat;
import de.unknownreality.dataframe.transform.CountTransformer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameTransformTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testTransform() throws IOException {
        String[] header = new String[]{"A", "B", "C", "D"};
        Integer[] col1 = new Integer[]{5, 3, 2, 4, 1};
        Double[] col2 = new Double[]{2d, 3d, null, 5d, 2d};
        String[] col3 = new String[]{"X", "Y", "X", Values.NA.toString(), "X"};
        Boolean[] col4 = new Boolean[]{false, false, true, false, true};
        String csv = createCSV(header, col1, col2, col3, col4);

        DataFrame df = DataFrameLoader.load(csv, FileFormat.TSV);


        DataFrame counts = df.getColumn("A").transform(new CountTransformer());
        for(DataRow row : counts){
            Assert.assertEquals(1,(int)row.getInteger(CountTransformer.COUNTS_COLUMN));
        }

        counts = df.getColumn("B").transform(new CountTransformer(false));
        counts.setPrimaryKey("B");
        Assert.assertEquals(4,counts.size());
        Assert.assertEquals(2,(int)counts.findByPrimaryKey(2d).getInteger(CountTransformer.COUNTS_COLUMN));
        Assert.assertEquals(1,(int)counts.findByPrimaryKey(3d).getInteger(CountTransformer.COUNTS_COLUMN));
        Assert.assertEquals(1,(int)counts.findByPrimaryKey(5d).getInteger(CountTransformer.COUNTS_COLUMN));
        Assert.assertEquals(1,(int)counts.findByPrimaryKey(5d).getInteger(CountTransformer.COUNTS_COLUMN));
        Assert.assertEquals(1,(int)counts.findByPrimaryKey(Values.NA).getInteger(CountTransformer.COUNTS_COLUMN));

        counts = df.getColumn("C").transform(new CountTransformer(true));
        counts.setPrimaryKey("C");
        Assert.assertEquals(2,counts.size());
        Assert.assertEquals(3,(int)counts.findByPrimaryKey("X").getInteger(CountTransformer.COUNTS_COLUMN));
        Assert.assertEquals(1,(int)counts.findByPrimaryKey("Y").getInteger(CountTransformer.COUNTS_COLUMN));

    }

    private String createCSV(String[] head, Object[]... cols) {
        StringBuilder sb = new StringBuilder();
        sb.append("");
        for (int i = 0; i < head.length; i++) {
            sb.append(head[i]);
            if (i < head.length - 1) {
                sb.append("\t");
            }
        }
        sb.append("\n");
        for (int i = 0; i < cols[0].length; i++) {
            for (int j = 0; j < head.length; j++) {
                sb.append(cols[j][i]);
                if (j < head.length - 1) {
                    sb.append("\t");
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
