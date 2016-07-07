package de.unknownreality.dataframe.common;

import java.io.File;
import java.util.Map;

/**
 * Created by Alex on 07.06.2016.
 */
public interface ReaderBuilder<H extends Header, R extends Row> {
    /**
     * Loads a map of attributes.
     * Used to create readers from data frame meta files
     *
     * @param attributes map of attributes
     * @throws Exception throws an exception if any error occurs
     */
    void loadAttributes(Map<String, String> attributes) throws Exception;

    /**
     * Creates a data container from a file.
     *
     * @param f file to be read
     * @return created data container
     */
    DataContainer<H, R> fromFile(File f);

    /**
     * Creates a data container from a string
     *
     * @param content string content used to create the data container
     * @return created data container
     */
    DataContainer<H, R> fromString(String content);
}
