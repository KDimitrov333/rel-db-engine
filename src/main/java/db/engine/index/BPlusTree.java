package db.engine.index;

import java.util.ArrayList;
import java.util.List;

public class BPlusTree {
    private final int order;              // max children per node
    private Node root;

    public BPlusTree(int order) {
        this.order = order;
        this.root = new Node(true); // start as empty leaf
    }

    // Insert key into recordId
    public void insert(int key, int recordId) {
        Node r = root;
        if (r.keys.size() == order - 1) {
            // root is full, split
            Node newRoot = new Node(false);
            newRoot.children.add(r);
            splitChild(newRoot, 0, r);
            root = newRoot;
        }
        insertNonFull(root, key, recordId);
    }

    // Search for key
    public List<Integer> search(int key) {
        return searchRecursive(root, key);
    }

    private List<Integer> searchRecursive(Node node, int key) {
        if (node.isLeaf) {
            int pos = firstGreaterPosition(node.keys, key); // lower bound (>= key)
            if (pos < node.keys.size() && node.keys.get(pos) == key) {
                return node.values.get(pos);
            }
            return new ArrayList<>();
        } else {
            int childIndex = firstGreaterOrEqualPosition(node.keys, key); // upper bound (> key)
            return searchRecursive(node.children.get(childIndex), key);
        }
    }

    // Insert into non-full node
    private void insertNonFull(Node node, int key, int recordId) {
        if (node.isLeaf) {
            leafInsert(node, key, recordId);
        } else {
            int pos = firstGreaterOrEqualPosition(node.keys, key);

            Node child = node.children.get(pos);
            if (child.keys.size() == order - 1) {
                splitChild(node, pos, child);
                if (key >= node.keys.get(pos)) {
                    pos++;
                }
            }
            insertNonFull(node.children.get(pos), key, recordId);
        }
    }

    // Insert into a leaf node at correct sorted position (or append to existing key's RID list)
    private void leafInsert(Node leaf, int key, int recordId) {
        int pos = firstGreaterPosition(leaf.keys, key);
        if (pos < leaf.keys.size() && leaf.keys.get(pos) == key) {
            leaf.values.get(pos).add(recordId);
        } else {
            leaf.keys.add(pos, key);
            List<Integer> list = new ArrayList<>();
            list.add(recordId);
            leaf.values.add(pos, list);
        }
    }

    // Split child
    private void splitChild(Node parent, int index, Node child) {
        if (child.isLeaf) {
            splitLeafChild(parent, index, child);
        } else {
            splitInternalChild(parent, index, child);
        }
    }

    // Split a full leaf node; promote first key of the new right sibling
    private void splitLeafChild(Node parent, int index, Node leaf) {
        int mid = (order - 1) / 2; // leaf retains [0..mid]
        Node sibling = new Node(true);

        // Copy keys/values to sibling (right side > mid)
        for (int i = mid + 1; i < leaf.keys.size(); i++) {
            sibling.keys.add(leaf.keys.get(i));
        }
        for (int i = mid + 1; i < leaf.values.size(); i++) {
            sibling.values.add(leaf.values.get(i));
        }

        // Maintain leaf chain
        sibling.next = leaf.next;
        leaf.next = sibling;

        // Shrink original leaf to keep keys/values up to mid inclusive
        while (leaf.keys.size() > mid + 1) leaf.keys.remove(leaf.keys.size() - 1);
        while (leaf.values.size() > mid + 1) leaf.values.remove(leaf.values.size() - 1);

        // Promote first key of sibling
        parent.keys.add(index, sibling.keys.get(0));
        parent.children.add(index + 1, sibling);
    }

    // Split a full internal node; promote median key; left keeps < median, right keeps > median
    private void splitInternalChild(Node parent, int index, Node internal) {
        int mid = (order - 1) / 2; // median key index
        int medianKey = internal.keys.get(mid); // capture before shrinking
        Node sibling = new Node(false);

        // Copy keys strictly greater than median to sibling
        for (int i = mid + 1; i < internal.keys.size(); i++) {
            sibling.keys.add(internal.keys.get(i));
        }
        // Copy corresponding child pointers to sibling (those to the right of median)
        for (int i = mid + 1; i < internal.children.size(); i++) {
            sibling.children.add(internal.children.get(i));
        }

        // Shrink original internal: keep keys [0..mid-1] and children [0..mid]
        while (internal.keys.size() > mid) internal.keys.remove(internal.keys.size() - 1);
        while (internal.children.size() > mid + 1) internal.children.remove(internal.children.size() - 1);

        // Insert median into parent between the two children
        parent.keys.add(index, medianKey);
        parent.children.add(index + 1, sibling);
    }

    // returns first position where existingKey > key (for leaf insert ordering)
    private int firstGreaterPosition(List<Integer> keys, int key) {
        // Binary search lower bound: first index where existingKey >= key
        int lo = 0, hi = keys.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (keys.get(mid) < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    // returns first position where existingKey >= key (for descent)
    private int firstGreaterOrEqualPosition(List<Integer> keys, int key) {
        // Binary search upper bound: first index where existingKey > key
        int lo = 0, hi = keys.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (keys.get(mid) <= key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    // Internal node structure
    private static final class Node {
        boolean isLeaf;
        List<Integer> keys;
        List<Node> children;        // internal nodes only
        List<List<Integer>> values; // leaf nodes only
        Node next;                  // link leaf nodes

        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.keys = new ArrayList<>();
            if (isLeaf) {
                this.values = new ArrayList<>();
                this.children = null;
            } else {
                this.children = new ArrayList<>();
                this.values = null;
            }
        }
    }
}
