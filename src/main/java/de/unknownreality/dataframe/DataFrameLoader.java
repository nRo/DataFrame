package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.ReaderBuilder;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Alex on 08.06.2016.
 */
public class DataFrameLoader {
    private static Logger logger = LoggerFactory.getLogger(DataFrameLoader.class);


    public static DataFrame load(File file) throws DataFrameLoaderException {
        File dataFile;
        File metaFile;
        String ext = "." + DataFrameMeta.META_FILE_EXTENSION;
        String filePath = file.getAbsolutePath();
        if (file.getName().endsWith(ext)) {
            metaFile = file;
            dataFile = new File(filePath
                    .substring(0, filePath.length() - ext.length()));
        } else {
            dataFile = file;
            metaFile = new File(filePath + ext);
        }
        return load(dataFile, metaFile);
    }

    public static DataFrame load(File file, File metaFile) throws DataFrameLoaderException {
        if (!file.exists()) {
            throw new DataFrameLoaderException(String.format("file not found %s", file.getAbsolutePath()));
        }
        if (!metaFile.exists()) {
            throw new DataFrameLoaderException(String.format("meta file not found %s", metaFile.getAbsolutePath()));

        }
        DataFrameMeta dataFrameMeta;
        try {
            dataFrameMeta = DataFrameMetaReader.read(metaFile);
        } catch (DataFrameMetaReader.DataFrameMetaReaderException ex) {
            throw new DataFrameLoaderException("error reading meta file", ex);
        }

        ReaderBuilder readerBuilder;
        try {
            readerBuilder = dataFrameMeta.getReaderBuilderClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameLoaderException("error creating readerBuilder instance", e);
        }
        try {
            readerBuilder.loadAttributes(dataFrameMeta.getAttributes());
        } catch (Exception e) {
            throw new DataFrameLoaderException("error loading readerBuilder attributes", e);
        }
        LinkedHashMap<String, DataFrameColumn> columns = createColumns(dataFrameMeta);
        DataContainer fileContainer = readerBuilder.fromFile(file);
        int i = 0;
        LinkedHashMap<String, DataFrameColumn> convertedColumns = new LinkedHashMap<>();
        for (Map.Entry<String, DataFrameColumn> entry : columns.entrySet()) {
            if (i == fileContainer.getHeader().size()) {
                throw new DataFrameLoaderException("columns count not matching meta file");
            }
            convertedColumns.put(fileContainer.getHeader().get(i).toString(), entry.getValue());
        }
        return DataFrameConverter.fromDataContainer(fileContainer, convertedColumns);
    }

    public static LinkedHashMap<String, DataFrameColumn> createColumns(DataFrameMeta dataFrameMeta) throws DataFrameLoaderException {
        LinkedHashMap<String, DataFrameColumn> columns = new LinkedHashMap<>();
        for (Map.Entry<String, Class<? extends DataFrameColumn>> entry : dataFrameMeta.getColumns().entrySet()) {
            String name = entry.getKey();
            Class<? extends DataFrameColumn> columnType = entry.getValue();
            DataFrameColumn column;
            try {
                column = columnType.newInstance();
            } catch (Exception e) {
                throw new DataFrameLoaderException("error creating column instance", e);
            }
            column.setName(name);
            columns.put(name, column);
        }
        return columns;
    }


    public static class DataFrameLoaderException extends DataFrameException {

        public DataFrameLoaderException(String message) {
            super(message);
        }

        public DataFrameLoaderException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }
}
