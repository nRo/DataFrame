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

package de.unknownreality.dataframe.join;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameLoader;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.group.DataFrameGroupingTest;
import de.unknownreality.dataframe.join.JoinedDataFrame;
import de.unknownreality.dataframe.join.impl.DefaultJoinUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameJoinTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testReader() throws IOException {
        /*
           GENE_ID;FPKM;CHR
            A;5;1
            B;4;2
            C;6;3
            D;6;1
         */


        CSVReader csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .setColumnType("GENE_ID",String.class)
                .setColumnType("FPKM",Double.class)
                .setColumnType("CHR",String.class)
                .build();

        DataFrame geneDataFrame = DataFrameLoader.load("data_join_a.csv", DataFrameGroupingTest.class.getClassLoader(), csvReader);
        Assert.assertEquals(4, geneDataFrame.size());


         /*
           TRANSCRIPT_ID;GENE_ID;FPKM;TRANSCRIPT_NUMBER
            TA;A;7;1
            TB;A;3;2
            TC;B;6;1
            TD;E;4;1
         */
        csvReader = CSVReaderBuilder.create()
                .withHeader(true)
                .withHeaderPrefix("")
                .withSeparator(';')
                .setColumnType("TRANSCRIPT_ID",String.class)
                .setColumnType("GENE_ID",String.class)
                .setColumnType("FPKM",Double.class)
                .setColumnType("TRANSCRIPT_NUMBER",Integer.class)
                .build();

        DataFrame transcriptDataFrame = DataFrameLoader.load("data_join_b.csv", DataFrameGroupingTest.class.getClassLoader(), csvReader);


        Assert.assertEquals(4, transcriptDataFrame.size());

        /*
           GENE_ID;FPKM.A;CHR;TRANSCRIPT_ID;FPKM.B;TRANSCRIPT_NUMBER
            A;5;1;TA;7;1
            A;5;1;TB;3;2
            B;4;2;TC;6;1
         */
        JoinedDataFrame innerJoin = geneDataFrame.joinInner(transcriptDataFrame, "GENE_ID");
        Assert.assertEquals(3, innerJoin.size());
        checkJoinedRow(innerJoin.getRow(0), "A", 5d, "1", "TA", 7d, 1);
        checkJoinedRow(innerJoin.getRow(1), "A", 5d, "1", "TB", 3d, 2);
        checkJoinedRow(innerJoin.getRow(2), "B", 4d, "2", "TC", 6d, 1);


        /*
           GENE_ID;FPKM.A;CHR;TRANSCRIPT_ID;FPKM.B;TRANSCRIPT_NUMBER
            A;5;1;TA;7;1
            A;5;1;TB;3;2
            B;4;2;TC;6;1
            C;;;;
            D;;;;
         */
        JoinedDataFrame leftJoin = geneDataFrame.joinLeft(transcriptDataFrame, "GENE_ID");
        Assert.assertEquals(5, leftJoin.size());
        checkJoinedRow(leftJoin.getRow(0), "A", 5d, "1", "TA", 7d, 1);
        checkJoinedRow(leftJoin.getRow(1), "A", 5d, "1", "TB", 3d, 2);
        checkJoinedRow(leftJoin.getRow(2), "B", 4d, "2", "TC", 6d, 1);
        checkJoinedRow(leftJoin.getRow(3), "C", 6d, "3", Values.NA, Values.NA, Values.NA);
        checkJoinedRow(leftJoin.getRow(4), "D", 6d, "1", Values.NA, Values.NA, Values.NA);


        /*
           GENE_ID;FPKM.A;CHR;TRANSCRIPT_ID;FPKM.B;TRANSCRIPT_NUMBER
            A;5;1;TA;7;1
            A;5;1;TB;3;2
            B;4;2;TC;6;1
            ;;;TD;E;4;1
         */
        JoinedDataFrame rightJoin = geneDataFrame.joinRight(transcriptDataFrame, "GENE_ID");
        Assert.assertEquals(4, rightJoin.size());
        checkJoinedRow(rightJoin.getRow(0), "A", 5d, "1", "TA", 7d, 1);
        checkJoinedRow(rightJoin.getRow(1), "A", 5d, "1", "TB", 3d, 2);
        checkJoinedRow(rightJoin.getRow(2), "B", 4d, "2", "TC", 6d, 1);
        checkJoinedRow(rightJoin.getRow(3), "E", Values.NA, Values.NA, "TD", 4d, 1);

        Assert.assertEquals(true,rightJoin.getJoinInfo().isB(transcriptDataFrame));
        Assert.assertEquals(false,rightJoin.getJoinInfo().isA(transcriptDataFrame));
        Assert.assertEquals(0,rightJoin.getJoinInfo().getJoinedIndex("GENE_ID",transcriptDataFrame));
        Assert.assertEquals("FPKM" + DefaultJoinUtil.JOIN_SUFFIX_B, rightJoin.getJoinInfo().getJoinedHeader("FPKM",transcriptDataFrame));
        Assert.assertEquals("FPKM" + DefaultJoinUtil.JOIN_SUFFIX_A, rightJoin.getJoinInfo().getJoinedHeaderA("FPKM"));
        Assert.assertEquals(4, rightJoin.getJoinInfo().getJoinedIndexB("FPKM"));

    }


    private static void checkJoinedRow(DataRow row, Object geneId, Object gene_fpkm, Object chr, Object transcriptId, Object transcript_fpkm, Object transcriptNumber) {
        Assert.assertEquals(geneId, row.get("GENE_ID"));
        Assert.assertEquals(gene_fpkm, row.get("FPKM" + DefaultJoinUtil.JOIN_SUFFIX_A));
        Assert.assertEquals(chr, row.get("CHR"));
        Assert.assertEquals(transcriptId, row.get("TRANSCRIPT_ID"));
        Assert.assertEquals(transcript_fpkm, row.get("FPKM" + DefaultJoinUtil.JOIN_SUFFIX_B));
        Assert.assertEquals(transcriptNumber, row.get("TRANSCRIPT_NUMBER"));
    }


}
