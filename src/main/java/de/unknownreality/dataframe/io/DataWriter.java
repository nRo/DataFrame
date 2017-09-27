/*
 *
 *  * Copyright (c) 2017 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.io;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataFrameWriter;
import de.unknownreality.dataframe.common.DataContainer;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex on 14.03.2016.
 */
public abstract class DataWriter {

    public void write(OutputStream os, DataContainer<?, ?> dataContainer) {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(os));
        write(bufferedWriter, dataContainer);
    }

    /**
     * Creates a {@link BufferedWriter}.
     *
     * @param writer target writer
     * @return {@link BufferedWriter}
     * @throws IOException
     */
    private BufferedWriter initWriter(Writer writer) throws IOException {

        if (writer instanceof BufferedWriter) {
            return (BufferedWriter) writer;
        }
        return new BufferedWriter(writer);
    }

    public void write(Writer writer, DataContainer<?,?> dataContainer){
        try {
            write(initWriter(writer),dataContainer);
        } catch (IOException e) {
            throw new DataFrameRuntimeException("error creating buffered writer",e);
        }
    }


    public abstract void write(BufferedWriter writer, DataContainer<?,?> dataContainer);

    /**
     * Writes a dataframe to a file
     * @param file target file
     * @param dataFrame dataframe to write
     * @param writeMeta write meta file
     * @deprecated use {@link de.unknownreality.dataframe.DataFrame#write} instead
     */
    @Deprecated
    public  void write(File file, DataFrame dataFrame, boolean writeMeta){
        DataFrameWriter.write(file,dataFrame,this, writeMeta);
    }


    public void write(File file, DataContainer<?, ?> dataContainer) {
        if (file.getParentFile() != null && !file.getParentFile().isDirectory()) {
            file.getParentFile().mkdirs();
        }
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file))) {
            write(bufferedWriter, dataContainer);
        } catch (IOException e) {
            throw new DataFrameRuntimeException(String.format("error writing file '%s'",file.getAbsolutePath()),e);
        }
    }

    public abstract Map<String,String> getSettings(DataFrame dataFrame);

    public abstract List<DataFrameColumn> getMetaColumns(DataFrame dataFrame);

    public abstract ReadFormat getReadFormat();

}
