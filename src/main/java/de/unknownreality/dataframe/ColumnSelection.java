package de.unknownreality.dataframe;

import de.unknownreality.dataframe.filter.FilterPredicate;

public class ColumnSelection<T extends DataFrame> {
    private DataFrameColumn[] columns;
    private T dataFrame;

    public ColumnSelection(T dataFrame, DataFrameColumn... columns){
        this.dataFrame = dataFrame;
        this.columns = columns;
    }

    /**
     * Returns a dataframe containing the selected columns and rows where a specified column value equals
     * an input value.
     *
     * @param colName column name
     * @param value   input value
     * @return new dataframe
     */
    public T where(String colName, Comparable value){
        DataRows rows = dataFrame.selectRows(colName,value);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and rows filtered by a{@link FilterPredicate}
     * an input value.
     *
     * @param predicate input predicate
     * @return new dataframe
     */
    public T where(FilterPredicate predicate){
        DataRows rows = dataFrame.selectRows(predicate);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and rows filtered by a predicate
     * an input value.
     *
     * @param predicateString input predicate string
     * @return new dataframe
     */
    public T where(String predicateString){
        DataRows rows = dataFrame.selectRows(predicateString);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and rows found using a specified index
     * an input value.
     *
     * @param indexName name of index
     * @param values index values
     * @return new dataframe
     */
    public T whereIndex(String indexName, Comparable... values){
        DataRows rows = dataFrame.selectRowsByIndex(indexName, values);
        return createDataFrame(rows);
    }

    /**
     * Returns a dataframe containing the selected columns and all rows from the original dataframe.
     *
     * @return new dataframe
     */
    public T allRows(){
        DataRows rows = dataFrame.getRows();
        return createDataFrame(rows);
    }

    @SuppressWarnings("unchecked")
    private T createDataFrame(DataRows rows){
        T df;
        try {
            df = (T)dataFrame.getClass().newInstance();
        } catch (Exception e) {
            throw new DataFrameRuntimeException("error creating dataframe instance",e);
        }
        for(DataFrameColumn column : columns){
            df.addColumn(column.copyEmpty());
        }
        DataRows newRows = new DataRows(df,rows);
        df.set(newRows);
        return df;
    }


}