package db.engine.index;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.catalog.TestCatalogManager;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

public class IndexManagerTest {

    @Test
    void createIndexAndLookup() {
        TestCatalogManager catalog = new TestCatalogManager();
        StorageManager storage = new StorageManager(catalog);
        TableSchema ts = new TableSchema("i_students", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        ), "target/i_students.tbl");
        new File(ts.filePath()).delete();
        storage.createTable(ts);
        storage.insert("i_students", new Record(List.of(1, "Alice", true)));
        storage.insert("i_students", new Record(List.of(2, "Bob", false)));
        storage.insert("i_students", new Record(List.of(2, "Bobby", true)));
        IndexManager im = new IndexManager(catalog, storage);
        im.createIndex("i_students_id_idx", "i_students", "id");
        var recs2 = im.lookup("i_students_id_idx", 2);
        assertEquals(2, recs2.size());
        assertEquals("Bob", recs2.get(0).getValues().get(1));
        assertEquals("Bobby", recs2.get(1).getValues().get(1));
        var range = im.rangeLookup("i_students_id_idx", 1, 2);
        assertEquals(3, range.size());
    }
}
