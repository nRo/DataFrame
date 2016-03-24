package de.unknownreality.data.frame.group;

import de.unknownreality.data.frame.filter.FilterPredicate;
import de.unknownreality.data.frame.sort.SortColumn;

import java.util.*;

/**
 * Created by Alex on 10.03.2016.
 */
public class DataGrouping implements Iterable<DataGroup>{
    private DataGroup[] groups;
    private String[] groupColumns;
    private Map<GroupKey,DataGroup> groupMap = new HashMap<>();

    public DataGrouping(Collection<DataGroup> groups,String... groupColumns) {
        this.groupColumns = groupColumns;
        this.groups = new DataGroup[groups.size()];
        groups.toArray(this.groups);
        for(DataGroup g : groups){
            groupMap.put(getKey(g.getGroupValues().getValues()),g);
        }
    }

    public DataGrouping concat(DataGrouping other){
        if(!Arrays.equals(this.getGroupColumns(),other.getGroupColumns())){
            throw new IllegalArgumentException("other DataGrouping must have the same GroupColumns");
        }
        DataGroup[] newGroups = new DataGroup[groups.length+other.size()];
        System.arraycopy(groups,0,newGroups,0,groups.length);
        int i = groups.length;
        for(DataGroup g : other){
            newGroups[i++] = g;
            groupMap.put(getKey(g.getGroupValues().getValues()),g);
        }
        return this;
    }
    private GroupKey getKey(Object... values){
        return new GroupKey(values);
    }

    public DataGroup findByGroupValues(Object... values){
        if(values.length != groupColumns.length){
            throw new IllegalArgumentException("values must have same length as GroupColumns");
        }
        return groupMap.get(getKey(values));
    }
    public String[] getGroupColumns() {
        return groupColumns;
    }

    public int size(){
        return groups.length;
    }


    public DataGrouping filter(FilterPredicate predicate){
        List<DataGroup> groups = findGroups(predicate);
        this.groups = new DataGroup[groups.size()];
        groups.toArray(this.groups);
        return this;
    }

    public DataGrouping find(FilterPredicate predicate){
        List<DataGroup> groups = findGroups(predicate);
        return new DataGrouping(groups,groupColumns);
    }

    public DataGrouping find(String colName,Comparable value){
        return find(FilterPredicate.eq(colName,value));
    }

    public DataGroup findFirst(String colName,Comparable value){
        return findFirst(FilterPredicate.eq(colName,value));

    }

    public DataGroup findFirst(FilterPredicate predicate){
        for(DataGroup row : this){
            if(predicate.valid(row.getGroupValues())){
                return row;
            }
        }
        return null;
    }

    public DataGrouping sort(SortColumn... columns){
        Arrays.sort(groups,new GroupValueComparator(columns));
        return this;
    }

    public DataGrouping sort(String name){
        return sort(name, SortColumn.Direction.Ascending);
    }
    public DataGrouping sort(String name, SortColumn.Direction dir){
        Arrays.sort(groups,new GroupValueComparator(new SortColumn[]{new SortColumn(name,dir)}));
        return this;
    }

    public DataGrouping sort(Comparator<DataGroup> comp){
        Arrays.sort(groups,comp);
        return this;
    }

    private List<DataGroup> findGroups(FilterPredicate predicate){
        List<DataGroup> groups = new ArrayList<>();
        for(DataGroup g : this) {
            if (predicate.valid(g.getGroupValues())) {
                groups.add(g);
            }
        }
        return groups;
    }
    @Override
    public Iterator<DataGroup> iterator() {
        return new Iterator<DataGroup>() {
            int index = 0;
            @Override
            public boolean hasNext() {
                return index < groups.length;
            }

            @Override
            public DataGroup next() {
                return groups[index++];
            }

            @Override
            public void remove() {

            }
        };
    }

    private static class GroupKey{
        private Object[] values;
        private int hash;
        private GroupKey(Object[] values){
            this.values = new Object[values.length];
            System.arraycopy(values,0,this.values,0,values.length);
            hash = Arrays.asList(values).hashCode();
        }

        public boolean equals(Object o){
            if(o == this){
                return true;
            }
            if(!(o instanceof  GroupKey)){
                return false;
            }
            GroupKey other = (GroupKey) o;
            if(hashCode() != other.hashCode()){
                return false;
            }
            return Arrays.equals(values,other.values);
        }

        public Object[] getValues() {
            return values;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
