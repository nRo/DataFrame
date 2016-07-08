package de.unknownreality.dataframe.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVResourceReader extends CSVReader {
    private static Logger log = LoggerFactory.getLogger(CSVResourceReader.class);
    private final String resourcePath;
    private final ClassLoader classLoader;
    private int skip = 0;

    /**
     * Creates a CSVResourceReader
     *
     * @param resourcePath   path to csv resource
     * @param classLoader    {@link ClassLoader} used to load the resource
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVResourceReader(String resourcePath, ClassLoader classLoader,Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        super(separator, containsHeader, headerPrefix, ignorePrefixes);
        this.resourcePath = resourcePath;
        this.classLoader = classLoader;
        initHeader();
        if (containsHeader) {
            skip++;
        }
    }

    /**
     * Creates a CSVResourceReader.
     * If no {@link ClassLoader} is provided. The class loader of CSVResourceReader is used.
     *
     * @param resourcePath   path to csv resource
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVResourceReader(String resourcePath,Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes){
        this(resourcePath,CSVResourceReader.class.getClassLoader(),separator,containsHeader,headerPrefix,ignorePrefixes);
    }

    /**
     * Returns the path to csv resource
     *
     * @return csv content
     */
    public String getResourcePath() {
        return resourcePath;
    }

    /**
     * Returns a {@link CSVIterator} for this string readers
     *
     * @return csv iterator
     */
    @Override
    public CSVIterator iterator() {

        return new CSVIterator(classLoader.getResourceAsStream(resourcePath)
                , getHeader(), getSeparator(), getIgnorePrefixes(), skip);
    }
}
