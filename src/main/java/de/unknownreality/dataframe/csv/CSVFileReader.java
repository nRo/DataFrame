package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.GZipUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 12.03.2016.
 */
public class CSVFileReader extends CSVReader {
    private static final Logger log = LoggerFactory.getLogger(CSVFileReader.class);
    private final File file;
    private final boolean gzipped;
    private int skip = 0;

    /**
     * Creates a CSVFileReader
     *
     * @param file           file to be read
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    public CSVFileReader(File file, Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        super(separator, containsHeader, headerPrefix, ignorePrefixes);
        this.file = file;
        gzipped = GZipUtil.isGzipped(file);
        initHeader();
        if (containsHeader) {
            skip++;
        }
    }

    /**
     * Returns a {@link CSVIterator} from this csv file reader
     *
     * @return csv row iterator
     */
    @Override
    public CSVIterator iterator() {
        try {
            InputStream inputStream = new FileInputStream(file);
            if (gzipped) {
                try {
                    inputStream = new GZIPInputStream(inputStream);
                } catch (IOException e) {
                    log.error("error creating gzip input stream", e);
                }
            }
            return new CSVIterator(inputStream, getHeader(), getSeparator(), getIgnorePrefixes(), skip);
        } catch (FileNotFoundException e) {
            log.error("file not found: {}", file, e);
        }
        return null;
    }
}
