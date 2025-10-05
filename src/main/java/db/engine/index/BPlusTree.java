package db.engine.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BPlusTree {
    private final int order;              // max children per internal node
    private final int maxKeys;            // order - 1
    private final int medianKeyIndex;     // cached median index for splits
    private Node root;

    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("B+ tree order must be >= 3 (got " + order + ")");
        }
        this.order = order;
        this.maxKeys = order - 1;
        this.medianKeyIndex = (order - 1) / 2; // used for splits
        this.root = new Node(true); // start as empty leaf
    }

    // Expose order for potential diagnostics / external validation
    public int getOrder() {
        return order;
    }

    // Search for key
    public List<Integer> search(int key) { return searchRecursive(root, key); }

    private List<Integer> searchRecursive(Node node, int key) {
        if (node.isLeaf) {
            int pos = lowerBound(node.keys, key); // first >= key
            if (pos < node.keys.size() && node.keys.get(pos) == key) {
                return List.copyOf(node.values.get(pos));
            }
            return Collections.emptyList();
        }
        int childIndex = upperBound(node.keys, key); // first > key
        return searchRecursive(node.children.get(childIndex), key);
    }

    // lowerBound: first index with keys[i] >= key (or keys.size() if none).
    // Used for leaf insertion and range scan start.
    private int lowerBound(List<Integer> keys, int key) {
        int lo = 0, hi = keys.size();
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (keys.get(mid) < key) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    // upperBound: first index with keys[i] > key (or keys.size() if none).
    // Used for internal routing: childIndex = upperBound(separators, key) where equal keys go right.
    private int upperBound(List<Integer> keys, int key) {
        int lo = 0, hi = keys.size();
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (keys.get(mid) <= key) lo = mid + 1; else hi = mid;
        }
        return lo;
    }


    //Range search
    /**
     * Range search: returns all record ids whose key is in [lowInclusive, highInclusive].
     * Keys returned preserve ascending key order; duplicate key record ids preserve insertion order.
     */
    public List<Integer> rangeSearch(int lowInclusive, int highInclusive) {
        List<Integer> result = new ArrayList<>();
        if (lowInclusive > highInclusive) return result;
        Node leaf = findLeaf(lowInclusive);
        if (leaf == null) return result;

        int pos = lowerBound(leaf.keys, lowInclusive); // first key >= low
        while (leaf != null) {
            for (int i = pos; i < leaf.keys.size(); i++) {
                int k = leaf.keys.get(i);
                if (k > highInclusive) return result; // past range
                result.addAll(leaf.values.get(i));
            }
            leaf = leaf.next;
            pos = 0; // restart at new leaf head
        }
        return result;
    }

    // Descend to the leaf that would contain the given key (first >= key or rightmost)
    private Node findLeaf(int key) {
        Node current = root;
        while (!current.isLeaf) {
            int childIndex = upperBound(current.keys, key); // first separator > key
            current = current.children.get(childIndex);
        }
        return current;
    }

    
    // Insert (key, recordId)
    public void insert(int key, int recordId) {
        Node r = root;
        if (r.keys.size() == maxKeys) {
            // root is full, split
            Node newRoot = new Node(false);
            newRoot.children.add(r);
            splitChild(newRoot, 0, r);
            root = newRoot;
        }
        insertNonFull(root, key, recordId);
    }

    // Insert into non-full node
    private void insertNonFull(Node node, int key, int recordId) {
        if (node.isLeaf) {
            leafInsert(node, key, recordId);
        } else {
            int pos = upperBound(node.keys, key);

            Node child = node.children.get(pos);
            if (child.keys.size() == maxKeys) {
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
        int pos = lowerBound(leaf.keys, key);
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
        int mid = medianKeyIndex;
        Node sibling = new Node(true);

        // Copy right side to sibling
        for (int i = mid + 1; i < leaf.keys.size(); i++) sibling.keys.add(leaf.keys.get(i));
        for (int i = mid + 1; i < leaf.values.size(); i++) sibling.values.add(leaf.values.get(i));

        // Link leaf chain
        sibling.next = leaf.next;
        leaf.next = sibling;

        // Trim original leaf (retain inclusive mid)
        while (leaf.keys.size() > mid + 1) leaf.keys.remove(leaf.keys.size() - 1);
        while (leaf.values.size() > mid + 1) leaf.values.remove(leaf.values.size() - 1);

        // Promote first key of sibling (copy-up style)
        parent.keys.add(index, sibling.keys.get(0));
        parent.children.add(index + 1, sibling);
    }

    // Split a full internal node; promote median key; left keeps < median, right keeps > median
    private void splitInternalChild(Node parent, int index, Node internal) {
        int mid = medianKeyIndex; // median separator
        int medianKey = internal.keys.get(mid);
        Node sibling = new Node(false);

        // Right sibling copies keys > median
        for (int i = mid + 1; i < internal.keys.size(); i++) sibling.keys.add(internal.keys.get(i));
        // Children to right of median
        for (int i = mid + 1; i < internal.children.size(); i++) sibling.children.add(internal.children.get(i));

        // Trim left node to keys < median and corresponding children
        while (internal.keys.size() > mid) internal.keys.remove(internal.keys.size() - 1);
        while (internal.children.size() > mid + 1) internal.children.remove(internal.children.size() - 1);

        // Parent takes median and pointer to new right sibling
        parent.keys.add(index, medianKey);
        parent.children.add(index + 1, sibling);
    }

    // Internal node structure
    private static final class Node {
        final boolean isLeaf;
        final List<Integer> keys;
        final List<Node> children;        // internal nodes only (null for leaves)
        final List<List<Integer>> values; // leaf nodes only (null for internals)
        Node next;                        // link leaf nodes (mutable linkage)

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
