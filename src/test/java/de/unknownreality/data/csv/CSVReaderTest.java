package de.unknownreality.data.csv;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVReaderTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testReader(){
        String testCSV = "#A\tB\tC\n1\tX\t3\n1\tX\t3\n";
        CSVReader reader = CSVReaderBuilder.create()
                .withHeaderPrefix("#")
                .containsHeader(true)
                .withSeparator('\t').load(testCSV);
        Assert.assertEquals("A",reader.getHeader().get(0));
        Assert.assertEquals("B",reader.getHeader().get(1));
        Assert.assertEquals("C",reader.getHeader().get(2));

        for(CSVRow row : reader){
            Assert.assertEquals("1",row.get(0));
            Assert.assertEquals("X",row.get(1));
            Assert.assertEquals("3",row.get(2));

            Assert.assertEquals("1",row.get("A"));
            Assert.assertEquals("X",row.get("B"));
            Assert.assertEquals("3",row.get("C"));
        }
    }
}
