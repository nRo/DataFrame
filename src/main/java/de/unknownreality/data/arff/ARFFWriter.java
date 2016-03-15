package de.unknownreality.data.arff;

import de.unknownreality.data.common.DataContainer;
import de.unknownreality.data.common.DataWriter;
import de.unknownreality.data.common.Header;
import de.unknownreality.data.common.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Alex on 14.03.2016.
 */
public class ARFFWriter implements DataWriter{
    private static Logger log = LoggerFactory.getLogger(ARFFWriter.class);
    private List<ARFFColumn> arffColumns = new ArrayList<>();
    private String relation;
    protected ARFFWriter(String relation, List<ARFFColumn> arffColumns){
        this.relation = relation;
        this.arffColumns = arffColumns;
    }
    @Override
    public void write(File file, DataContainer<? extends Header,? extends Row> dataContainer) {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))){
            writer.write("@relation ");
            writer.write(relation);
            writer.newLine();
            for(ARFFColumn column : arffColumns){
                writer.write("@ATTRIBUTE ");
                writer.write(column.getARFFColumn());
                writer.write(" ");
                if(column.getType() != ARFFType.Nominal){
                    writer.write(column.getType().name().toUpperCase());
                }
                else{
                    Set<String> values = new HashSet<>();
                    for(Row row : dataContainer){
                        values.add(row.get(column.getDataContainerColumn()).toString());
                    }
                    writer.write("{");
                    int i = 0;
                    for(String s : values){
                        writer.write(s);
                        if(i++ < values.size() -1){
                            writer.write(",");
                        }
                    }
                    writer.write("}");
                }
                writer.newLine();
            }
            writer.newLine();
            for(Row row : dataContainer){
                for(ARFFColumn column : arffColumns){
                    if(column.getType() == ARFFType.Numeric){
                        writer.write(row.getDouble(column.getDataContainerColumn()).toString());
                    }
                    else{
                        writer.write(row.get(column.getDataContainerColumn()).toString());
                    }
                }
                writer.newLine();
            }
        } catch (IOException e) {
            log.error("error writing {}",file,e);
        }
    }
}
