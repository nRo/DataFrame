package de.unknownreality.data.frame;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.ReaderBuilder;
import de.unknownreality.data.frame.meta.DataFrameMeta;
import de.unknownreality.data.frame.meta.DataFrameMetaReader;
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


    public static DataFrame load(File file){
        File dataFile;
        File metaFile;
        String ext = "." + DataFrameMeta.META_FILE_EXTENSION;
        String filePath = file.getAbsolutePath();
        if(file.getName().endsWith(ext)){
            metaFile = file;
            dataFile = new File(filePath
                    .substring(0,filePath.length() - ext.length()));
        }
        else{
            dataFile = file;
            metaFile = new File(filePath+ext);
        }
        return load(dataFile,metaFile);
    }

    public static DataFrame load(File file, File metaFile){
        if(!file.exists()){
            logger.error(String.format("file not found %s",file.getAbsolutePath()));
            return null;
        }
        if(!metaFile.exists()){
            logger.error(String.format("meta file not found %s",metaFile.getAbsolutePath()));
            return null;
        }
        DataFrameMeta dataFrameMeta;
        try{
            dataFrameMeta = DataFrameMetaReader.read(metaFile);
        }
        catch (DataFrameMetaReader.DataFrameMetaReaderException ex){
            logger.error("error creating meta file",ex);
            return null;
        }

        ReaderBuilder readerBuilder;
        try{
            readerBuilder = dataFrameMeta.getReaderBuilderClass().newInstance();
        }
        catch (InstantiationException e) {
            logger.error("error creating readerBuilder instance",e);
            return null;
        } catch (IllegalAccessException e) {
            logger.error("error creating readerBuilder instance",e);
            return null;
        }
        try{
            readerBuilder.loadAttributes(dataFrameMeta.getAttributes());
        }
        catch (Exception e){
            logger.error("error loading readerBuilder attributes",e);
            return null;
        }
        LinkedHashMap<String,DataFrameColumn> columns = new LinkedHashMap<>();
        for(Map.Entry<String,Class<? extends DataFrameColumn>> entry : dataFrameMeta.getColumns().entrySet()){
            String name = entry.getKey();
            Class<? extends DataFrameColumn> columnType = entry.getValue();
            DataFrameColumn column;
            try{
                column = columnType.newInstance();
            }
            catch (InstantiationException e) {
                logger.error("error creating column instance",e);
                return null;
            } catch (IllegalAccessException e) {
                logger.error("error creating column instance",e);
                return null;
            }
            column.setName(name);
            columns.put(name,column);
        }
        DataContainer fileContainer = readerBuilder.fromFile(file);
        return DataFrameConverter.fromDataContainer(fileContainer,columns);
    }





    public static class DataFrameLoaderException extends Exception{
        public DataFrameLoaderException() {}

        public DataFrameLoaderException(String message)
        {
            super(message);
        }
    }
}
