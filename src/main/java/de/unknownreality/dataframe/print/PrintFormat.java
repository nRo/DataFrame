package de.unknownreality.dataframe.print;

import de.unknownreality.dataframe.io.WriteFormat;
import de.unknownreality.dataframe.io.WriterBuilder;

public class PrintFormat implements WriteFormat {
    @Override
    public WriterBuilder getWriterBuilder() {
        return PrinterBuilder.create();
    }
}
