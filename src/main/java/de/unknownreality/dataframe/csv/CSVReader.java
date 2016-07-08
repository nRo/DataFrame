package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.mapping.*;

import de.unknownreality.dataframe.DataFrameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public abstract class CSVReader implements DataContainer<CSVHeader, CSVRow> {
    private static final Logger log = LoggerFactory.getLogger(CSVReader.class);
    private String headerPrefix = "#";
    private final Character separator;
    private final CSVHeader header = new CSVHeader();
    private boolean containsHeader;
    private final String[] ignorePrefixes;

    /**
     * Creates a CSVIterator
     *
     * @param separator      csv column separator
     * @param containsHeader specifies whether file contains header row
     * @param headerPrefix   specifies the prefix of the header row
     * @param ignorePrefixes array of prefixes for lines that should be ignored
     */
    protected CSVReader(Character separator, boolean containsHeader, String headerPrefix, String[] ignorePrefixes) {
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
        this.ignorePrefixes = ignorePrefixes;
    }

    /**
     * Returns <tt>true</tt> if this reader considers a header row
     *
     * @return <tt>true</tt> if header row exists
     */
    public boolean containsHeader() {
        return containsHeader;
    }

    /**
     * Returns the prefix for the header line used by this reader
     *
     * @return header line prefix
     */
    public String getHeaderPrefix() {
        return headerPrefix;
    }

    /**
     * Returns the csv row separator used by this reader
     *
     * @return csv row separator
     */
    public Character getSeparator() {
        return separator;
    }

    /**
     * Returns the csv header created by this reader
     *
     * @return csv header
     */
    public CSVHeader getHeader() {
        return header;
    }

    /**
     * Returns the array of prefixes for lines ignored by this reader
     *
     * @return prefixes array
     */
    public String[] getIgnorePrefixes() {
        return ignorePrefixes;
    }

    /**
     * Reads and created the csv header
     */
    public void initHeader() {
        try {
            CSVIterator iterator = iterator();
            CSVRow row = iterator.next();
            if (row == null) {
                containsHeader = false;
                return;
            }
            if (containsHeader) {
                if (!row.get(0).startsWith(headerPrefix)) {
                    throw new CSVException("invalid header prefix in first line");
                }
                for (int i = 0; i < row.size(); i++) {
                    String name = row.get(i);
                    if (i == 0) {
                        name = headerPrefix == null ? name : name.substring(headerPrefix.length());
                    }
                    header.add(name);
                }
            } else {
                for (int i = 0; i < row.size(); i++) {
                    header.add();
                }
                containsHeader = false;
            }
            iterator.close();

        } catch (CSVException e) {
            log.error("error creating csv header", e);

        }
    }

    /**
     * Returns a {@link CSVIterator} for this reader
     *
     * @return csv iterator
     */
    @Override
    public abstract CSVIterator iterator();

    /**
     * Converts this reader into a {@link DataFrameBuilder}.
     *
     * @return {@link DataFrameBuilder} for this csv reader.
     */
    public DataFrameBuilder toDataFrame() {
        return DataFrameBuilder.create(this);
    }


    public <T> List<T> map(Class<T> cl) {
        return DataMapper.map(this, cl);
    }


}
