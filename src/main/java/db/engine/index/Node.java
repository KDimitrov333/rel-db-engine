package db.engine.index;

import java.util.ArrayList;
import java.util.List;

class Node {
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
