package de.unknownreality.dataframe.frame.sort;

/**
 * Created by Alex on 09.03.2016.
 */
public class SortColumn {
    public enum Direction{
        Ascending,
        Descending
    }
    private String name;
    private Direction direction;
    public SortColumn(String name,Direction direction){
        this.name = name;
        this.direction = direction;
    }

    public SortColumn(String name){
        this.name = name;
        this.direction = Direction.Ascending;
    }

    public String getName() {
        return name;
    }

    public Direction getDirection() {
        return direction;
    }
}
