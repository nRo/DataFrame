package de.unknownreality.data.arff;

import de.unknownreality.data.csv.CSVWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 09.03.2016.
 */
public class ARFFWriterBuilder {
    private String relation;
    private List<ARFFColumn> arffColumnList = new ArrayList<>();


    public static ARFFWriterBuilder create() {
        return new ARFFWriterBuilder();
    }


    public ARFFWriterBuilder setRelation(String relation) {
        this.relation = relation;
        return this;
    }

    public ARFFWriterBuilder addColumn(String name, String dataName,ARFFType type){
        arffColumnList.add(new ARFFColumn(name,dataName,type));
        return this;
    }

    public ARFFWriterBuilder addColumn(String name, ARFFType type){
        arffColumnList.add(new ARFFColumn(name,name,type));
        return this;
    }



    public ARFFWriter build() {
        return new ARFFWriter(relation,arffColumnList);
    }


}
