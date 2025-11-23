package db.engine.index;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.storage.RID;

public class BPlusTreeTest {
    @Test
    void insertSearchDuplicateKeys() {
        BPlusTree tree = new BPlusTree(4);
        RID r1 = new RID(0,0);
        RID r2 = new RID(0,1);
        RID r3 = new RID(1,5);
        tree.insert(10, r1);
        tree.insert(10, r2);
        tree.insert(20, r3);
        List<RID> ten = tree.search(10);
        assertEquals(2, ten.size());
        assertTrue(ten.contains(r1));
        assertTrue(ten.contains(r2));
        List<RID> twenty = tree.search(20);
        assertEquals(List.of(r3), twenty);
        assertTrue(tree.search(5).isEmpty());
    }

    @Test
    void rangeSearchLimits() {
        BPlusTree tree = new BPlusTree(4);
        for (int i=0;i<50;i++) tree.insert(i, new RID(i/10, i));
        List<RID> range = tree.rangeSearch(5, 12); // inclusive
        assertEquals(8, range.size()); // keys 5..12 each have one rid
        assertTrue(range.get(0).slotId() >= 5);
        assertTrue(range.get(range.size()-1).slotId() <= 12);
    }

    @Test
    void deleteRemovesOneRidAndKeyWhenEmpty() {
        BPlusTree tree = new BPlusTree(4);
        RID r1 = new RID(0,0);
        RID r2 = new RID(0,1);
        tree.insert(7, r1);
        tree.insert(7, r2);
        assertEquals(2, tree.search(7).size());
        assertTrue(tree.delete(7, r1));
        assertEquals(1, tree.search(7).size());
        assertTrue(tree.delete(7, r2));
        assertTrue(tree.search(7).isEmpty());
        assertFalse(tree.delete(7, r2)); // already gone
    }
}
