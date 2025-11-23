package db.engine.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

public class RecordTest {
    @Test
    void valuesAreStoredAndRetrieved() {
        Record r = new Record(List.of(1, "Alice", true));
        assertEquals(3, r.getValues().size());
        assertEquals(1, r.getValues().get(0));
        assertEquals("Alice", r.getValues().get(1));
        assertEquals(true, r.getValues().get(2));
    }

    @Test
    void equalsDifferentInstancesSameValues() {
        Record r1 = new Record(List.of(2, "Bob", false));
        Record r2 = new Record(List.of(2, "Bob", false));
        assertNotSame(r1, r2);
        assertEquals(r1.getValues(), r2.getValues());
    }
}
