package de.unknownreality.data.arff;

/**
 * Created by Alex on 15.03.2016.
 */
public class ARFFColumn  {
    private String arffColumn;
    private String dataContainerColumn;
    private ARFFType type;

    public ARFFColumn(String name, String dataCol,ARFFType type){
        this.arffColumn = name;
        this.type = type;
        this.dataContainerColumn = dataCol;
    }
    public ARFFColumn(String name, ARFFType type){
        this.arffColumn = name;
        this.type = type;
        this.dataContainerColumn = name;
    }
    public ARFFType getType() {
        return type;
    }

    public String getARFFColumn() {
        return arffColumn;
    }

    public String getDataContainerColumn() {
        return dataContainerColumn;
    }
}
