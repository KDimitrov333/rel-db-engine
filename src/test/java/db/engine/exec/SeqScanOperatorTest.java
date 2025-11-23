package db.engine.exec;

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

public class SeqScanOperatorTest {

    @Test
    void scansAllInsertedRows() {
        TestCatalogManager catalog = new TestCatalogManager();
        StorageManager storage = new StorageManager(catalog);
        TableSchema ts = new TableSchema("scan_students", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        ), "target/scan_students.tbl");
        new File(ts.filePath()).delete();
        storage.createTable(ts);
        for (int i=1;i<=10;i++) {
            storage.insert("scan_students", new Record(List.of(i, "S"+i, i % 2 == 0)));
        }
        SeqScanOperator scan = new SeqScanOperator(storage, "scan_students");
        scan.open();
        int count = 0;
        while (scan.next() != null) {
            count++;
        }
        scan.close();
        assertEquals(10, count);
    }
}
