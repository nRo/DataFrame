package de.unknownreality.data.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by Alex on 02.06.2016.
 */
public class GZipUtil {
    private static Logger log = LoggerFactory.getLogger(GZipUtil.class);
    public static boolean isGzipped(File file){
        try{
            return isGZipped(new FileInputStream(file));
        }
        catch (Exception e){
            log.error("error opening file",e);
        }
        return false;
    }

    public static boolean isGzipped(String string, String encoding){
        try{
            return isGZipped(new ByteArrayInputStream(string.getBytes(encoding)));
        }
        catch (Exception e){
            log.error("error opening file",e);
        }
        return false;
    }

    public static boolean isGZipped(InputStream in) {
        if (!in.markSupported()) {
            in = new BufferedInputStream(in);
        }
        in.mark(2);
        int magic = 0;
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
