package de.unknownreality.dataframe.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 02.06.2016.
 */
public class GZipUtil {
    private static final Logger log = LoggerFactory.getLogger(GZipUtil.class);

    /**
     * Returns <tt>true</tt> if the specified file is gzipped
     *
     * @param file file to test
     * @return <tt>true</tt> if file is gzipped
     */
    public static boolean isGzipped(File file) {
        try {
            return isGZipped(new FileInputStream(file));
        } catch (Exception e) {
            log.error("error opening file", e);
        }
        return false;
    }

    /**
     * Returns <tt>true</tt> if specified {@link InputStream} is gzipped
     *
     * @param in Input stream to test
     * @return <tt>true</tt> if input stream is gzipped
     */
    public static boolean isGZipped(InputStream in) {
        if (!in.markSupported()) {
            in = new BufferedInputStream(in);
        }
        in.mark(2);
        int magic;
        try {
            magic = in.read() & 0xff | ((in.read() << 8) & 0xff00);
            in.reset();
        } catch (IOException e) {
            e.printStackTrace(System.err);
            return false;
        }
        return magic == GZIPInputStream.GZIP_MAGIC;
    }
}
