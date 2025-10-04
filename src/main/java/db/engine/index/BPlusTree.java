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
        int mid = (order - 1) / 2;
        Node sibling = new Node(child.isLeaf);

        // move half the keys
        for (int i = mid + 1; i < child.keys.size(); i++) {
            sibling.keys.add(child.keys.get(i));
        }

        if (child.isLeaf) {
            for (int i = mid + 1; i < child.values.size(); i++) {
                sibling.values.add(child.values.get(i));
            }
            // maintain leaf links
            sibling.next = child.next;
            child.next = sibling;

            // shrink child
            while (child.keys.size() > mid + 1) child.keys.remove(child.keys.size() - 1);
            while (child.values.size() > mid + 1) child.values.remove(child.values.size() - 1);

            // insert new key in parent
            parent.keys.add(index, sibling.keys.get(0));
            parent.children.add(index + 1, sibling);
        } else {
            for (int i = mid + 1; i < child.children.size(); i++) {
                sibling.children.add(child.children.get(i));
            }

            // shrink child
            while (child.keys.size() > mid) child.keys.remove(child.keys.size() - 1);
            while (child.children.size() > mid + 1) child.children.remove(child.children.size() - 1);

            // insert new key in parent
            parent.keys.add(index, child.keys.get(mid));
            parent.children.add(index + 1, sibling);
        }
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
