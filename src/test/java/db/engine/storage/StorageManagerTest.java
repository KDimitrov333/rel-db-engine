package db.engine.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.catalog.TestCatalogManager;

public class StorageManagerTest {

    private List<ColumnSchema> schemaCols() {
        return List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        );
    }

    @Test
    void createInsertScanDeleteLifecycle() {
        TestCatalogManager catalog = new TestCatalogManager();
        StorageManager storage = new StorageManager(catalog);
        TableSchema ts = new TableSchema("people", schemaCols(), "target/test-people.tbl");
        new File(ts.filePath()).delete(); // ensure fresh
        storage.createTable(ts);
        storage.insert("people", new Record(List.of(1, "Alice", true)));
        storage.insert("people", new Record(List.of(2, "Bob", false)));
        List<Record> all = storage.scanTable("people");
        assertEquals(2, all.size());
        // Delete Bob
        RID bobRid = new RID(0,1); // second slot on first page
        assertTrue(storage.delete("people", bobRid));
        List<Record> remaining = storage.scanTable("people");
        assertEquals(1, remaining.size());
        assertEquals(1, remaining.get(0).getValues().get(0));
    }
}
