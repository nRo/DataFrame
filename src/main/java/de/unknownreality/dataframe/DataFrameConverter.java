package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.DataContainer;
import de.unknownreality.dataframe.common.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Alex on 09.03.2016.
 */
public class DataFrameConverter {
    private static final Logger log = LoggerFactory.getLogger(DataFrameConverter.class);
    public static final int MAX_PARSER_CACHE_SIZE = 10000;

    @SuppressWarnings("unchecked")
    public static DataFrame fromDataContainer(DataContainer<?, ?> reader, LinkedHashMap<String, DataFrameColumn> columns) {
        Map<String, Object> parserCache = new HashMap<>();
        int[] colIndices = new int[columns.size()];
        int i = 0;
        for (String h : columns.keySet()) {
            colIndices[i] = reader.getHeader().getIndex(h);
            i++;
        }
        for (Row row : reader) {
            i = 0;
            for (Map.Entry<String, DataFrameColumn> columnEntry : columns.entrySet()) {
                String strVal = row.getString(colIndices[i++]);
                if (strVal == null || "".equals(strVal) || "null".equals(strVal)) {
                    columnEntry.getValue().doAppendNA();
                    continue;
                }
                try {
                    if (Values.NA.isNA(strVal)) {
                        columnEntry.getValue().doAppendNA();
                        continue;
                    }
                    Object o;
                    String object_ident = columnEntry.getValue().getType() + "::" + strVal;
                    if ((o = parserCache.get(object_ident)) == null) {
                        o = columnEntry.getValue().getParser().parse(strVal);
                        if (parserCache.size() > MAX_PARSER_CACHE_SIZE) {
                            parserCache.clear();
                        }
                        parserCache.put(object_ident, o);
                    }
                    if (o == null || !(o instanceof Comparable)) {
                        columnEntry.getValue().doAppendNA();
                        continue;
                    }
                    columnEntry.getValue().append(o);
                } catch (ParseException e) {
                    log.warn("error parsing value, NA added", e);
                    columnEntry.getValue().doAppendNA();
                }
            }
        }
        DataFrame dataFrame = new DataFrame();
        for (DataFrameColumn column : columns.values()) {
            dataFrame.addColumn(column);
        }
        return dataFrame;
    }
}
