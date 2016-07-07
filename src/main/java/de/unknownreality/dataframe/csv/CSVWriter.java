package de.unknownreality.dataframe.csv;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.DataWriter;
import de.unknownreality.dataframe.common.Header;
import de.unknownreality.dataframe.common.Row;
import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.meta.DataFrameMeta;
import de.unknownreality.dataframe.meta.DataFrameMetaWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Alex on 14.03.2016.
 */
public class CSVWriter implements DataWriter {
    private static final Logger log = LoggerFactory.getLogger(CSVWriter.class);

    private char separator = '\t';
    private String headerPrefix = "#";
    private boolean containsHeader = true;
    private boolean gzip = false;

    /**
     * Creates a csv writer
     *
     * @param separator      csv column separator char
     * @param containsHeader write csv header line
     * @param headerPrefix   csv header line prefix
     * @param gzip           use gzip
     */
    protected CSVWriter(char separator, boolean containsHeader, String headerPrefix, boolean gzip) {
        this.separator = separator;
        this.containsHeader = containsHeader;
        this.headerPrefix = headerPrefix;
        this.gzip = gzip;
    }

    /**
     * Writes a csv file
     *
     * @param file          target file
     * @param dataContainer data container
     * @param separator     separator char
     */
    public static void write(File file, DataContainer dataContainer, char separator) {
        write(file, dataContainer, separator, true, "", true);
    }

    /**
     * Writes a csv file
     *
     * @param file           target file
     * @param dataContainer  data container
     * @param separator      separator char
     * @param containsHeader write header line
     * @param headerPrefix   header line prefix
     */
    public static void write(File file, DataContainer dataContainer, char separator, boolean containsHeader, String headerPrefix) {
        write(file, dataContainer, separator, containsHeader, headerPrefix, false);
    }


    /**
     * Writes a csv file
     *
     * @param file           target file
     * @param dataContainer  data container
     * @param separator      separator char
     * @param containsHeader write header line
     * @param headerPrefix   header line prefix
     * @param gzip           use gzip
     */
    public static void write(File file, DataContainer<?, ?> dataContainer, char separator, boolean containsHeader, String headerPrefix, boolean gzip) {
        new CSVWriter(separator, containsHeader, headerPrefix, gzip).write(file, dataContainer);
    }

    /**
     * Prints a csv file to {@link System#out}
     *
     * @param dataContainer  data container
     * @param separator      separator char
     * @param containsHeader write header line
     * @param headerPrefix   header line prefix
     */
    public static void print(DataContainer<?, ?> dataContainer, char separator, boolean containsHeader, String headerPrefix) {
        new CSVWriter(separator, containsHeader, headerPrefix, false).print(dataContainer);
    }


    @Override
    public void write(File file, DataContainer<? extends Header, ? extends Row> dataContainer) {
        try (BufferedWriter writer = initWriter(file)) {
            if (containsHeader) {
                if (headerPrefix != null) {
                    writer.write(headerPrefix);
                }
                for (int i = 0; i < dataContainer.getHeader().size(); i++) {
                    writer.write(dataContainer.getHeader().get(i).toString());
                    if (i < dataContainer.getHeader().size() - 1) {
                        writer.write(separator);
                    }
                }
                writer.newLine();
            }
            for (Row row : dataContainer) {
                for (int i = 0; i < row.size(); i++) {
                    writer.write(row.get(i).toString());
                    if (i < row.size() - 1) {
                        writer.write(separator);
                    }
                }
                writer.newLine();
            }
        } catch (IOException e) {
            log.error("error writing {}", file, e);
        }
    }

    @Override
    public void write(File file, DataFrame dataFrame, boolean writeMetaFile) {
        write(file, dataFrame);
        if (writeMetaFile) {
            writeMetaFile(dataFrame, new File(file.getAbsolutePath() + ".dfm"));
        }
    }

    /**
     * Writes a meta file for a data frame
     *
     * @param dataFrame target data frame
     * @param file      target file
     */
    private void writeMetaFile(DataFrame dataFrame, File file) {
        DataFrameMeta metaFile = DataFrameMeta.create(
                dataFrame, CSVReaderBuilder.class, getAttributes()
        );
        DataFrameMetaWriter.write(metaFile, file);
    }

    /**
     * Creates a {@link BufferedWriter}.
     * If specified a gzip writer is created.
     *
     * @param file target file
     * @return {@link BufferedWriter}
     * @throws IOException
     */
    private BufferedWriter initWriter(File file) throws IOException {
        OutputStream outputStream;
        if (!file.getParentFile().isDirectory()) {
            file.getParentFile().mkdirs();
        }
        if (gzip) {
            outputStream = new GZIPOutputStream(
                    new FileOutputStream(file));
        } else {
            outputStream = new FileOutputStream(file);
        }
        return new BufferedWriter(
                new OutputStreamWriter(outputStream, "UTF-8"));

    }


    @Override
    public void print(DataContainer<? extends Header, ? extends Row> dataContainer) {
        if (containsHeader) {
            if (headerPrefix != null) {
                System.out.print(headerPrefix);
            }
            for (int i = 0; i < dataContainer.getHeader().size(); i++) {
                System.out.print(dataContainer.getHeader().get(i).toString());
                if (i < dataContainer.getHeader().size() - 1) {
                    System.out.print(separator);
                }
            }
            System.out.println();
        }
        for (Row row : dataContainer) {
            for (int i = 0; i < row.size(); i++) {
                System.out.print(row.get(i).toString());
                if (i < row.size() - 1) {
                    System.out.print(separator);
                }
            }
            System.out.println();
        }
    }

    /**
     * Returns the attributes map used by {@link CSVReaderBuilder}.
     *
     * @return attributes map
     * @see CSVReaderBuilder#loadAttributes(Map)
     */
    @Override
    public Map<String, String> getAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("separator", Character.toString(separator));
        attributes.put("headerPrefix", headerPrefix);
        attributes.put("containsHeader", Boolean.toString(containsHeader));
        attributes.put("gzip", Boolean.toString(gzip));
        return attributes;
    }
}
