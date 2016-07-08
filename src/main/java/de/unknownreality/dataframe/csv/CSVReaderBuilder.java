package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.ReaderBuilder;
import de.unknownreality.dataframe.common.parser.ParserUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 09.03.2016.
 */
public class CSVReaderBuilder implements ReaderBuilder<CSVHeader, CSVRow> {
    private Character separator = '\t';
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private final List<String> ignorePrefixes = new ArrayList<>();

    /**
     * Creates csv reader builder instance
     *
     * @return csv reader
     */
    public static CSVReaderBuilder create() {
        return new CSVReaderBuilder();
    }

    public CSVReaderBuilder() {

    }

    /**
     * Sets the csv column separator char used by the resulting reader
     * <p>default: <tt>'\t'</tt></p>
     *
     * @param separator csv column separator
     * @return <tt>self</tt> for method chaining
     */
    public CSVReaderBuilder withSeparator(Character separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Specifies whether the resulting reader considers a header line
     * <p>default: <tt>true</tt></p>
     *
     * @param containsHeader header line parameter
     * @return <tt>self</tt> for method chaining
     */
    public CSVReaderBuilder containsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
        return this;
    }

    /**
     * Adds an ignore line prefix. Lines beginning with this prefix are ignored by the resulting reader
     *
     * @param prefix ignore line prefix
     * @return <tt>self</tt> for method chaining
     */
    public CSVReaderBuilder addIgnorePrefix(String prefix) {
        ignorePrefixes.add(prefix);
        return this;
    }


    /**
     * Sets the header prefix used by the resulting reader. Only important if header line exists.
     * <p>default:<tt>#</tt></p>
     *
     * @param headerPrefix header line prefix
     * @return <tt>self</tt> for method chaining
     */
    public CSVReaderBuilder withHeaderPrefix(String headerPrefix) {
        this.headerPrefix = headerPrefix;
        return this;
    }

    public Character getSeparator() {
        return separator;
    }

    public String getHeaderPrefix() {
        return headerPrefix;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }


    /**
     * Creates a {@link CSVReader} for the specified file
     *
     * @param file source file
     * @return csv reader for source file
     */
    public CSVReader load(File file) {
        String[] ignorePrefixesArray = new String[this.ignorePrefixes.size()];
        this.ignorePrefixes.toArray(ignorePrefixesArray);
        return new CSVFileReader(file, getSeparator(), isContainsHeader(), getHeaderPrefix(), ignorePrefixesArray);
    }

    /**
     * Creates a {@link CSVReader} for the specified csv content string
     *
     * @param content csv content string
     * @return csv reader for content string
     */
    public CSVReader load(String content) {
        String[] ignorePrefixesArray = new String[this.ignorePrefixes.size()];
        this.ignorePrefixes.toArray(ignorePrefixesArray);
        return new CSVStringReader(content, getSeparator(), isContainsHeader(), getHeaderPrefix(), ignorePrefixesArray);
    }

    /**
     * Creates a {@link CSVReader} for a specified resource.
     * The provided {@link ClassLoader} is used to load the resource.
     *
     * @param resourcePath path to csv resource
     * @param classLoader {@link ClassLoader} used to load the resource
     * @return
     */
    public CSVReader loadResource(String resourcePath,ClassLoader classLoader) {
        String[] ignorePrefixesArray = new String[this.ignorePrefixes.size()];
        this.ignorePrefixes.toArray(ignorePrefixesArray);
        return new CSVResourceReader(resourcePath,classLoader, getSeparator(), isContainsHeader(), getHeaderPrefix(), ignorePrefixesArray);
    }

    /**
     * Creates a {@link CSVReader} for a specified resource.
     * The {@link ClassLoader} of {@link CSVResourceReader} is used to load the resource.
     *
     * @param resourcePath path to csv resource
     * @return
     */
    public CSVReader loadResource(String resourcePath) {
        String[] ignorePrefixesArray = new String[this.ignorePrefixes.size()];
        this.ignorePrefixes.toArray(ignorePrefixesArray);
        return new CSVResourceReader(resourcePath, getSeparator(), isContainsHeader(), getHeaderPrefix(), ignorePrefixesArray);
    }

    /**
     * Sets all attributes using a map
     * <p>"separator" = csv column separator</p>
     * <p>"headerPrefix" = header line prefix</p>
     * <p>"containsHeader" = reader considers header line (<tt>true</tt> or <tt>false</tt>)</p>
     *
     * @param attributes map of attributes
     * @throws Exception throws an exception if an attribute can not be correctly parsed
     */
    @Override
    public void loadAttributes(Map<String, String> attributes) throws Exception {
        this.separator = ParserUtil.parse(Character.class, attributes.get("separator"));
        this.headerPrefix = attributes.get("headerPrefix");
        this.containsHeader = ParserUtil.parse(Boolean.class, attributes.get("containsHeader"));
    }

    /**
     * @see #load(File)
     */
    @Override
    public DataContainer<CSVHeader, CSVRow> fromFile(File f) {
        return load(f);
    }

    /**
     * @see #load(String)
     */
    @Override
    public DataContainer<CSVHeader, CSVRow> fromString(String content) {
        return load(content);
    }
}
