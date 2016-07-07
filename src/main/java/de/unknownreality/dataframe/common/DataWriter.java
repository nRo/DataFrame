package de.unknownreality.dataframe.common;

import de.unknownreality.dataframe.DataFrame;

import java.io.File;
import java.util.Map;

/**
 * Created by Alex on 14.03.2016.
 */
public interface DataWriter {
    /**
     * Writes a data container into a file
     *
     * @param file          target file
     * @param dataContainer container to write
     */
    void write(File file, DataContainer<? extends Header, ? extends Row> dataContainer);

    /**
     * Writes a {@link DataFrame} into a file and if specified also writes a meta file
     *
     * @param file          target file
     * @param dataFrame     data frame to write
     * @param writeMetaFile write a meta file parameter
     */
    void write(File file, DataFrame dataFrame, boolean writeMetaFile);

    /**
     * Prints a data container to {@link System#out}
     *
     * @param dataContainer data container to print
     */
    void print(DataContainer<? extends Header, ? extends Row> dataContainer);

    /**
     * Returns a attributes map used by the corresponding reader builder
     *
     * @return attributes map
     * @see ReaderBuilder#loadAttributes(Map)
     */
    Map<String, String> getAttributes();
}
