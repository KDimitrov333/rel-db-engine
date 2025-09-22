package db.engine.index;

import java.util.ArrayList;
import java.util.List;

public class BPlusTree {
    private int order;              // max children per node
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
            for (int i = 0; i < node.keys.size(); i++) {
                if (node.keys.get(i) == key) {
                    return node.values.get(i);
                }
            }
            return new ArrayList<>();
        } else {
            int i = 0;
            while (i < node.keys.size() && key >= node.keys.get(i)) {
                i++;
            }
            return searchRecursive(node.children.get(i), key);
        }
    }

    // Insert into non-full node
    private void insertNonFull(Node node, int key, int recordId) {
        if (node.isLeaf) {
            int pos = 0;
            while (pos < node.keys.size() && key > node.keys.get(pos)) pos++;

            if (pos < node.keys.size() && node.keys.get(pos) == key) {
                node.values.get(pos).add(recordId);
            } else {
                node.keys.add(pos, key);
                List<Integer> list = new ArrayList<>();
                list.add(recordId);
                node.values.add(pos, list);
            }
        } else {
            int pos = 0;
            while (pos < node.keys.size() && key >= node.keys.get(pos)) pos++;

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
}
