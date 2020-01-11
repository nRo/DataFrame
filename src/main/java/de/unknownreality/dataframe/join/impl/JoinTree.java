package de.unknownreality.dataframe.join.impl;

import de.unknownreality.dataframe.DataFrame;
import de.unknownreality.dataframe.DataRow;
import de.unknownreality.dataframe.join.JoinColumn;

import java.util.*;

public class JoinTree {
    public enum SafeLeafMode{
        All,
        FirstOnly
    }
    Set<JoinNode> savedLeafs = new LinkedHashSet<>();

    private JoinNode root = new JoinNode(null);
    private boolean saveLeafsA;
    private boolean saveLeafsB;
    private int[] colIndicesA;
    private int[] colIndicesB;

    public JoinTree(SafeLeafMode mode,DataFrame dfA, DataFrame dfB, JoinColumn... columns) {
        int i = 0;
        colIndicesA = new int[columns.length];
        colIndicesB = new int[columns.length];
        for (JoinColumn column : columns) {
            int idx = i++;
            colIndicesA[idx] = dfA.getHeader().getIndex(column.getColumnA());
            colIndicesB[idx] = dfB.getHeader().getIndex(column.getColumnB());
        }
        this.saveLeafsA = true;
        this.saveLeafsB = mode == SafeLeafMode.All;
        setA(dfA);
        setB(dfB);
    }

    private void setA(DataFrame df) {
        set(df, colIndicesA,true, saveLeafsA);
    }

    private void setB(DataFrame df) {
        set(df, colIndicesB,false,saveLeafsB);
    }

    private void set(DataFrame df,int[] colIndices, boolean isA, boolean safeLeafs) {
        for (DataRow row : df) {
            Comparable<?>[] values = createValues(row,colIndices);
            addRec(root, 0, values, row.getIndex(),isA,safeLeafs);
        }
    }


    private void addRec(JoinNode node, int index, Comparable<?>[] values, Integer rowIndex, boolean isA, boolean saveLeaf) {
        if (index == values.length) {
            if(saveLeaf) {
                savedLeafs.add(node);
            }
            if(isA) {
                node.addIndexA(rowIndex);
            }
            else{
                node.addIndexB(rowIndex);
            }
            return;
        }
        Comparable<?> value = values[index];
        JoinNode child;
        if ((child = node.getChild(value)) == null) {
            child = new JoinNode(value);
            node.addChild(child);
        }
        addRec(child, index + 1, values, rowIndex, isA, saveLeaf);
    }

    private Comparable<?> [] createValues(DataRow dataRow, int[] colIndices) {
        Comparable<?> [] values = new Comparable<?>[colIndices.length];
        for(int i = 0; i < colIndices.length; i++){
            values[i] = dataRow.get(colIndices[i]);
        }
        return values;
    }


    public Set<JoinNode> getSavedLeafs() {
        return savedLeafs;
    }

    public class JoinNode {
        private Comparable<?>  value;
        private HashMap<Comparable<?> , JoinNode> children;
        private List<Integer> indicesA;
        private List<Integer> indicesB;

        public JoinNode(Comparable<?>  value) {
            this.value = value;
        }

        public void clear() {
            if (children != null) {
                children.clear();
            }
            if (indicesA != null) {
                indicesA.clear();
            }
            if (indicesB != null) {
                indicesB.clear();
            }
        }

        private HashMap<Comparable<?> , JoinNode> getChildrenMap() {
            if (children == null) {
                children = new HashMap<>();
            }
            return children;
        }

        public Comparable<?>  getValue() {
            return value;
        }


        public void addChild(JoinNode child) {
            getChildrenMap().put(child.getValue(), child);
        }

        public JoinNode getChild(Comparable<?>  value) {
            return getChildrenMap().get(value);
        }

        public void removeChild(JoinNode child) {
            removeChild(child.getValue());
        }

        public void removeChild(Comparable<?>  value) {
            getChildrenMap().remove(value);
        }

        public void addIndexA(Integer index) {
            if(indicesA == null){
                indicesA = new ArrayList<>();
            }
            getIndicesA().add(index);
        }

        public void addIndexB(Integer index) {
            if(indicesB == null){
                indicesB = new ArrayList<>();
            }
            getIndicesB().add(index);
        }

        public boolean hasChildren() {
            return children != null && !children.isEmpty();
        }


        public Collection<Integer> getIndicesA() {
            return indicesA;
        }

        public Collection<Integer> getIndicesB() {
            return indicesB;
        }
    }
}
