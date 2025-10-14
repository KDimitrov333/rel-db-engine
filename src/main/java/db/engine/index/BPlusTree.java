package db.engine.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import db.engine.storage.RID;

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

    // Search for key - return immutable list of RIDs (may be empty)
    public List<RID> search(int key) { return searchRecursive(root, key); }

    private List<RID> searchRecursive(Node node, int key) {
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
    public List<RID> rangeSearch(int lowInclusive, int highInclusive) {
        List<RID> result = new ArrayList<>();
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

    
    // Insert (key, PageRID)
    public void insert(int key, RID rid) {
        Node r = root;
        if (r.keys.size() == maxKeys) {
            // root is full, split
            Node newRoot = new Node(false);
            newRoot.children.add(r);
            splitChild(newRoot, 0, r);
            root = newRoot;
        }
        insertNonFull(root, key, rid);
    }

    /**
     * Delete one (key,rid) pair.
     * No rebalancing or separator key fixes; key removed if last rid list entry;
     * root may shrink only in trivial cases. Returns true if removed, false otherwise.
     */
    public boolean delete(int key, RID rid) {
         // Descend to leaf
         List<Node> path = new ArrayList<>();
         Node current = root;
         while (!current.isLeaf) {
             path.add(current);
             int childIndex = upperBound(current.keys, key);
             current = current.children.get(childIndex);
         }
         int pos = lowerBound(current.keys, key);
         if (pos >= current.keys.size() || current.keys.get(pos) != key) {
             return false; // key not found
         }
    List<RID> rids = current.values.get(pos);
         boolean removed = rids.remove(rid);
         if (!removed) return false; // rid not present
         if (rids.isEmpty()) {
             current.keys.remove(pos);
             current.values.remove(pos);
             // Root shrink (will decide if necessary later)
             if (root != current && root.keys.isEmpty() && !root.isLeaf && root.children.size() == 1) {
                 root = root.children.get(0);
             } else if (root == current && root.keys.isEmpty() && root.isLeaf) {
                 // all entries removed -> empty tree
             }
         }
         return true;
     }

    // Insert into non-full node
    private void insertNonFull(Node node, int key, RID rid) {
        if (node.isLeaf) {
            leafInsert(node, key, rid);
        } else {
            int pos = upperBound(node.keys, key);

            Node child = node.children.get(pos);
            if (child.keys.size() == maxKeys) {
                splitChild(node, pos, child);
                if (key >= node.keys.get(pos)) {
                    pos++;
                }
            }
            insertNonFull(node.children.get(pos), key, rid);
        }
    }

    // Insert into a leaf node at correct sorted position (or append to existing key's RID list)
    private void leafInsert(Node leaf, int key, RID rid) {
        int pos = lowerBound(leaf.keys, key);
        if (pos < leaf.keys.size() && leaf.keys.get(pos) == key) {
            leaf.values.get(pos).add(rid);
        } else {
            leaf.keys.add(pos, key);
            List<RID> list = new ArrayList<>();
            list.add(rid);
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
        int total = leaf.keys.size(); // == maxKeys prior to split
        Node sibling = new Node(true);

        // Balanced split: left gets ceil(total/2), right gets the rest.
        int leftSize = (total + 1) / 2; // ensures left >= right when odd
        int rightSize = total - leftSize;

        // Move rightSize entries from end of leaf to sibling (preserve order)
        List<Integer> moveKeys = new ArrayList<>(rightSize);
    List<List<RID>> moveVals = new ArrayList<>(rightSize);
        for (int i = total - rightSize; i < total; i++) {
            moveKeys.add(leaf.keys.get(i));
            moveVals.add(leaf.values.get(i));
        }

        // Trim leaf down to leftSize
        while (leaf.keys.size() > leftSize) leaf.keys.remove(leaf.keys.size() - 1);
        while (leaf.values.size() > leftSize) leaf.values.remove(leaf.values.size() - 1);

        // Assign sibling contents
        sibling.keys.addAll(moveKeys);
        sibling.values.addAll(moveVals);

        // Chain leaves
        sibling.next = leaf.next;
        leaf.next = sibling;

        // Promote first key of sibling to parent
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
    final List<List<RID>> values;    // leaf nodes only (null for internals)
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
