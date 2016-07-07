package de.unknownreality.dataframe.meta;

import de.unknownreality.dataframe.common.DataWriter;
import de.unknownreality.dataframe.common.ReaderBuilder;
import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Alex on 07.06.2016.
 */
public class DataFrameMeta {
    public static final String META_FILE_EXTENSION = "dfm";

    private Class<? extends ReaderBuilder> readerBuilderClass;
    private Map<String, String> attributes = new HashMap<>();
    private LinkedHashMap<String, Class<? extends DataFrameColumn>> columns = new LinkedHashMap<>();

    public static DataFrameMeta create(DataFrame dataFrame, Class<? extends ReaderBuilder> readerBuilderClass, DataWriter dataWriterBuilder) {
        return create(dataFrame, readerBuilderClass, dataWriterBuilder.getAttributes());
    }

    public static DataFrameMeta create(DataFrame dataFrame, Class<? extends ReaderBuilder> readerBuilderClass, Map<String, String> writerAttributes) {
        DataFrameMeta dataFrameMetaFile = new DataFrameMeta();
        dataFrameMetaFile.readerBuilderClass = readerBuilderClass;
        dataFrameMetaFile.attributes = writerAttributes;
        for (String header : dataFrame.getHeader()) {
            dataFrameMetaFile.columns.put(header, dataFrame.getHeader().getColumnType(header));
        }
        return dataFrameMetaFile;
    }

    public Class<? extends ReaderBuilder> getReaderBuilderClass() {
        return readerBuilderClass;
    }

    public LinkedHashMap<String, Class<? extends DataFrameColumn>> getColumns() {
        return columns;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public DataFrameMeta() {

    }

    public DataFrameMeta(LinkedHashMap<String, Class<? extends DataFrameColumn>> columns,
                         Class<? extends ReaderBuilder> readerBuilderClass, Map<String, String> attributes) {
        this.columns = columns;
        this.readerBuilderClass = readerBuilderClass;
        this.attributes = attributes;
    }
}
