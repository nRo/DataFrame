package de.unknownreality.dataframe.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVStringReader extends CSVReader {
    private static Logger log = LoggerFactory.getLogger(CSVStringReader.class);
    private final String content;
    private int skip = 0;

    /**
     * Creates a CSVFileReader
     *
     * @param content        csv string content
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVStringReader(String content, Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        super(separator, containsHeader, headerPrefix, ignorePrefixes);
        this.content = content;
        initHeader();
        if (containsHeader) {
            skip++;
        }

    }

    /**
     * Returns the string content of this csv string reader
     *
     * @return csv content
     */
    public String getContent() {
        return content;
    }

    /**
     * Returns a {@link CSVIterator} for this string readers
     *
     * @return csv iterator
     */
    @Override
    public CSVIterator iterator() {

        return new CSVIterator(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
                , getHeader(), getSeparator(), getIgnorePrefixes(), skip);
    }
}
