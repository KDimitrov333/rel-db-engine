package db.engine.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;

public class HeapPageTest {
    private static final int PAGE_SIZE = 4096;

    private List<ColumnSchema> schema() {
        return List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        );
    }

    @Test
    void insertAndReadRoundTrip() {
        byte[] data = new byte[PAGE_SIZE];
        HeapPage page = HeapPage.wrap("heap-test", 0, data, PAGE_SIZE);
        Record r1 = new Record(List.of(1, "Alice", true));
        Record r2 = new Record(List.of(2, "Bob", false));
        int s1 = page.insert(r1.toBytes(schema()));
        int s2 = page.insert(r2.toBytes(schema()));
        assertEquals(0, s1);
        assertEquals(1, s2);
        Record rr1 = page.readRecord(s1, schema());
        Record rr2 = page.readRecord(s2, schema());
        assertEquals(r1.getValues(), rr1.getValues());
        assertEquals(r2.getValues(), rr2.getValues());
        assertEquals(List.of(0,1), page.liveSlotIds());
    }

    @Test
    void deleteTombstonesSlot() {
        byte[] data = new byte[PAGE_SIZE];
        HeapPage page = HeapPage.wrap("heap-test", 0, data, PAGE_SIZE);
        Record r1 = new Record(List.of(1, "Alice", true));
        int s1 = page.insert(r1.toBytes(schema()));
        assertEquals(List.of(0), page.liveSlotIds());
        page.delete(s1);
        assertTrue(page.liveSlotIds().isEmpty());
        // Reading tombstoned slot should throw
        assertThrows(IllegalStateException.class, () -> page.readRecord(s1, schema()));
    }
}
