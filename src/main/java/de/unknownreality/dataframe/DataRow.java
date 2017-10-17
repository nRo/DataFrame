package de.unknownreality.dataframe;

import de.unknownreality.dataframe.common.row.UpdateableRow;

/**
 * Created by Alex on 12.06.2017.
 */
public class DataRow extends UpdateableRow<String,DataFrameHeader,Comparable> {
    private DataFrame dataFrame;
    private int size;
    private int rowVersion;
    public DataRow(DataFrame dataFrame, int index) {
        super(dataFrame.getHeader(),index);
        this.rowVersion = dataFrame.getVersion();
        this.size = getHeader().size();
        this.dataFrame = dataFrame;
    }

    public boolean isVersionValid(){
        return rowVersion == dataFrame.getVersion();
    }

    private void checkValidity(){
        if(!isVersionValid()){
            throw new DataFrameRuntimeException("row is no longer valid, the dataframe changed since the row object was created");
        }
    }

    @Override
    public Comparable get(int index) {
        checkValidity();
        if(dataFrame.isNA(index,getIndex())){
            return Values.NA;
        }
        return dataFrame.getValue(index,getIndex());
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Checks whether a certain value is compatible for a column.
     * Compatible means that the value has a type suited for the respective class
     * @param value checked value
     * @param headerName header name
     * @return true if value is compatible
     */
    @Override
    public boolean isCompatible(Comparable value, String headerName) {
        Class<? extends Comparable> type = getHeader().getType(headerName);
        if(Number.class.isAssignableFrom(type)){
            return Number.class.isAssignableFrom(value.getClass());
        }
        return type.isAssignableFrom(value.getClass());
    }

    /**
     * Checks whether a certain value is compatible for a column.
     * Compatible means that the value has a type suited for the respective class
     * @param value checked value
     * @param headerIndex header index
     * @return true if value is compatible
     */
    @Override
    public boolean isCompatible(Comparable value, int headerIndex) {
        Class<? extends Comparable> type = getHeader().getType(headerIndex);
        return type.isAssignableFrom(value.getClass());
    }

    @Override
    protected void setValue(int index, Comparable value) {
        checkValidity();
        dataFrame.setValue(index,getIndex(),value);
    }


    /**
     * Returns the values of a row at a specified index
     *
     * @param i index of data row
     * @return values in data row
     */
    public Comparable[] getRowValues(int i) {
        checkValidity();
        if (i >= dataFrame.size()) {
            throw new DataFrameRuntimeException("index out of bounds");
        }
        Comparable[] values = new Comparable[dataFrame.getHeader().size()];
        for (int j = 0; j < dataFrame.getHeader().size(); j++) {
            Comparable value = dataFrame.getValue(j,i);
            if (Values.NA.isNA(value)) {
                values[j] = Values.NA;
            } else {
                values[j] = value;
            }
        }
        return values;
    }

}
