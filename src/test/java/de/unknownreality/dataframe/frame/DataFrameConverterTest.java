package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

public class DataFrameConverterTest {
    @Test
    public void testAutoDetection(){

        StringBuilder csvString = new StringBuilder();
        csvString.append("x;y;v;b\n");
        for(int i = 0; i < 1000;i++){
            csvString.append(String.format(Locale.US,"%d;%f;%s;%b",i,i*0.7,i+"s",true));
            csvString.append("\n");
        }

        DataFrame df = DataFrame.fromCSV(csvString.toString(),';',true);
        Assert.assertEquals(Integer.class,df.getHeader().getType("x"));
        Assert.assertEquals(Double.class,df.getHeader().getType("y"));
        Assert.assertEquals(String.class,df.getHeader().getType("v"));
        Assert.assertEquals(Boolean.class,df.getHeader().getType("b"));


        csvString = new StringBuilder();
        csvString.append("x;y;v;b\n");
        for(int i = 0; i < 1000;i++){
            if(i == 101){
                csvString.append(String.format(Locale.US,"%s;%f;%s;%b","x",i*0.7,i+"s",true));
            }
            else{
                csvString.append(String.format(Locale.US,"%d;%f;%s;%b",i,i*0.7,i+"s",true));
            }
            csvString.append("\n");
        }

        df = DataFrame.fromCSV(csvString.toString(),';',true);
        Assert.assertEquals(String.class,df.getHeader().getType("x"));
        Assert.assertEquals(Double.class,df.getHeader().getType("y"));
        Assert.assertEquals(String.class,df.getHeader().getType("v"));
        Assert.assertEquals(Boolean.class,df.getHeader().getType("b"));
    }
}
