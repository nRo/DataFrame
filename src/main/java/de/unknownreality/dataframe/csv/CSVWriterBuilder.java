package de.unknownreality.dataframe.csv;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVWriterBuilder {
    private char separator = '\t';
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private boolean gzip = false;

    /**
     * Creates a csv writer builder instance
     *
     * @return csv writer
     */
    public static CSVWriterBuilder create() {
        return new CSVWriterBuilder();
    }


    /**
     * Sets the csv column separator char used by the resulting writer
     * <p>default: <tt>'\t'</tt></p>
     *
     * @param separator csv column separator
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder withSeparator(char separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Specifies whether the resulting writer appends a header line
     * <p>default: <tt>true</tt></p>
     *
     * @param containsHeader header line parameter
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }


    /**
     * Sets the header prefix used by the resulting reader. Only important if header line exists.
     * <p>default:<tt>#</tt></p>
     *
     * @param headerPrefix header line prefix
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    /**
     * Specifies whether the resulting writer uses gzip
     * <p>default: <tt>false</tt></p>
     *
     * @param gzip header line parameter
     * @return <tt>self</tt> for method chaining
     */
    public CSVWriterBuilder useGzip(boolean gzip) {
        this.gzip = gzip;
        return this;
    }


    public char getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }

    public boolean isGzip() {
        return gzip;
    }

    /**
     * Creates a csv writer using the specified settings
     *
     * @return csv writer
     */
    public CSVWriter build() {
        return new CSVWriter(getSeparator(), isContainsHeader(), getHeaderPrefix(), isGzip());
    }


}
