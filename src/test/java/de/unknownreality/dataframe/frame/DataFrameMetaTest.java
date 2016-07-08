package de.unknownreality.dataframe.frame;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.Values;
import de.unknownreality.dataframe.column.*;
import de.unknownreality.dataframe.common.ReaderBuilder;
import de.unknownreality.dataframe.csv.CSVReader;
import de.unknownreality.dataframe.csv.CSVReaderBuilder;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 12.03.2016.
 */
public class DataFrameMetaTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testMetaReader() throws Exception {
        DataFrameMeta meta = DataFrameMetaReader.read(DataFrameMetaTest.class.getResourceAsStream("/legacy_meta.dfm"));
        Assert.assertEquals("#", meta.getAttributes().get("headerPrefix"));
        Assert.assertEquals("\t", meta.getAttributes().get("separator"));
        Assert.assertEquals("true", meta.getAttributes().get("gzip"));
        Assert.assertEquals("false", meta.getAttributes().get("containsHeader"));


        ReaderBuilder readerBuilder = meta.getReaderBuilderClass().newInstance();
        Assert.assertEquals(CSVReaderBuilder.class, readerBuilder.getClass());
        CSVReaderBuilder csvReaderBuilder = (CSVReaderBuilder) readerBuilder;
        csvReaderBuilder.loadAttributes(meta.getAttributes());


        Assert.assertEquals("#", csvReaderBuilder.getHeaderPrefix());
        Assert.assertEquals(new Character('\t'), csvReaderBuilder.getSeparator());
        Assert.assertEquals(false, csvReaderBuilder.isContainsHeader());

        Assert.assertEquals(IntegerColumn.class,meta.getColumns().get("id"));
        Assert.assertEquals(DoubleColumn.class,meta.getColumns().get("value"));
        Assert.assertEquals(StringColumn.class,meta.getColumns().get("description"));

    }

}
