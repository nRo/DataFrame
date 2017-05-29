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

package de.unknownreality.dataframe.index;

import de.unknownreality.dataframe.DataFrameColumn;
import de.unknownreality.dataframe.DataFrameRuntimeException;
import de.unknownreality.dataframe.DataRow;

import java.util.*;

/**
 * Created by Alex on 27.05.2016.
 */
public class TreeIndex implements Index{
    private final Map<Integer, TreeNode> indexNodeMap = new HashMap<>();
    private TreeNode root = new TreeNode(null, null);

    private final Map<DataFrameColumn, Integer> columnIndexMap = new LinkedHashMap<>();
    private final String name;
    private boolean unique = false;
    /**
     * Creates a multi index using more that one column
     *
     * @param indexName name of index
     * @param columns   index columns
     */
    protected TreeIndex(String indexName,boolean unique, DataFrameColumn... columns) {
        int i = 0;
        for (DataFrameColumn column : columns) {
            columnIndexMap.put(column, i++);
        }
        this.name = indexName;
        this.unique = unique;
    }

    protected TreeIndex(String indexName,DataFrameColumn... columns) {
        this(indexName,false,columns);
    }

    @Override
    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void clear() {
        indexNodeMap.clear();
        root.clear();
    }

    @Override
    public boolean containsColumn(DataFrameColumn column) {
        return columnIndexMap.containsKey(column);
    }

    @Override
    public List<DataFrameColumn> getColumns() {
        return new ArrayList<>(columnIndexMap.keySet());
    }

    @Override
    public void update(DataRow dataRow) {
        remove(dataRow);
        Comparable[] values = createValues(dataRow);
        addRec(root, 0, values, dataRow.getIndex());
    }

    private void addRec(TreeNode node, int index, Comparable[] values, Integer rowIndex) {
        if (index == values.length) {
            if(unique && node.hasIndices()){
                throw new DataFrameRuntimeException(String.format("error adding row to index: duplicated values found '%s'", Arrays.toString(values)));
            }
            indexNodeMap.put(rowIndex,node);
            node.addIndex(rowIndex);
            return;
        }
        Comparable value = values[index];
        TreeNode child;
        if ((child = node.getChild(value)) == null) {
            child = new TreeNode(node, value);
            node.addChild(child);
        }
        addRec(child, index + 1, values, rowIndex);
    }


    @Override
    public void remove(DataRow dataRow) {
        TreeNode node = indexNodeMap.get(dataRow.getIndex());
        if (node == null) {
            return;
        }
        node.removeIndex(dataRow.getIndex());
        removeRec(node);
    }

    private void removeRec(TreeNode node) {
        if (node.hasIndices() || node.hasChildren()) {
            return;
        }
        if (node.getParent() != null) {
            TreeNode parent = node.getParent();
            parent.removeChild(node.getValue());
            removeRec(parent);
        }
    }

    private Comparable[] createValues(DataRow dataRow) {
        Comparable[] values = new Comparable[columnIndexMap.size()];
        int i = 0;
        for (DataFrameColumn column : columnIndexMap.keySet()) {
            values[i++] = dataRow.get(column.getName());
        }
        return values;
    }

    @Override
    public Collection<Integer> find(Comparable... values) {
        if (values.length != columnIndexMap.size()) {
            throw new IllegalArgumentException("value for each index column required");
        }
        TreeNode node = findRec(root, 0, values);
        if (node == null || !node.hasIndices()) {
            return new ArrayList<>(0);
        }
        return node.getIndices();
    }


    private TreeNode findRec(TreeNode node, int index, Comparable[] values) {
        if (index == values.length) {
            return node;
        }
        Comparable value = values[index];
        TreeNode child;
        if ((child = node.getChild(value)) == null) {
            return null;
        }
        return findRec(child, index + 1, values);

    }

    private class TreeNode {
        private Comparable value;
        private HashMap<Comparable, TreeNode> children;
        private List<Integer> indices;
        private TreeNode parent;

        public TreeNode(TreeNode parent, Comparable value) {
            this.value = value;
            this.parent = parent;
        }

        public TreeNode getParent() {
            return parent;
        }

        public void clear() {
            if (children != null) {
                children.clear();
            }
            if (indices != null) {
                indices.clear();
            }
        }

        private HashMap<Comparable, TreeNode> getChildrenMap() {
            if (children == null) {
                children = new HashMap<>();
            }
            return children;
        }

        public Collection<TreeNode> getChildren() {
            return getChildrenMap().values();
        }

        public Comparable getValue() {
            return value;
        }


        public void addChild(TreeNode child) {
            getChildrenMap().put(child.getValue(), child);
        }

        public TreeNode getChild(Comparable value) {
            return getChildrenMap().get(value);
        }

        public void removeChild(TreeNode child) {
            removeChild(child.getValue());

        }

        public void removeChild(Comparable value) {
            getChildrenMap().remove(value);
        }

        public void addIndex(Integer index) {
            getIndices().add(index);
        }

        public boolean hasChildren() {
            return children != null && !children.isEmpty();
        }

        public boolean hasIndices() {
            return indices != null && !indices.isEmpty();
        }

        public void removeIndex(Integer index) {
            getIndices().remove(index);
        }

        public Collection<Integer> getIndices() {
            if (indices == null) {
                indices = new ArrayList<>();
            }
            return indices;
        }
    }

}
