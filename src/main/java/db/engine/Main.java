package db.engine;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.index.IndexManager;
import db.engine.storage.Record;
import db.engine.storage.RID;
import db.engine.storage.StorageManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {

        // Deterministic environment cleanup for demo purposes
        deleteIfExists("data/students.tbl");
        deleteIfExists("indexes/students_id_idx.idx");

        CatalogManager catalog = new CatalogManager();
        StorageManager storage = new StorageManager(catalog);
        IndexManager index = new IndexManager(catalog, storage);

        TableSchema schema = new TableSchema(
            "students",
            List.of(
                new ColumnSchema("id", DataType.INT, 0),
                new ColumnSchema("name", DataType.VARCHAR, 50),
                new ColumnSchema("active", DataType.BOOLEAN, 0)
            ),
            "data/students.tbl"
        );

        storage.createTable(schema);
        System.out.println("Created table 'students'\n");

        // Insert sample data (including duplicate key to show multi-value index capability)
        List<Record> seed = List.of(
            new Record(List.of(1, "Alice", true)),
            new Record(List.of(2, "Bob", false)),
            new Record(List.of(2, "Bobby", true)), // duplicate id
            new Record(List.of(3, "Eve", true)),
            new Record(List.of(4, "Dave", false))
        );

    List<RID> rids = new ArrayList<>();
        for (Record r : seed) {
            RID rid = storage.insert("students", r);
            rids.add(rid);
        }
        System.out.println("Inserted " + seed.size() + " rows (with duplicate id=2)\n");

        // Full heap scan
        System.out.println("Full heap scan (pageId,slotId: record):");
        List<Record> all = new ArrayList<>();
        storage.scan("students", (rid, rec) -> {
            System.out.println("(" + rid.pageId() + "," + rid.slotId() + ") : " + rec);
            all.add(rec);
        });
        check(all.size() == seed.size(), "Scan count should equal inserted count");

        // Build index
        index.createIndex("students_id_idx", "students", "id");
        System.out.println("\nCreated B+Tree index 'students_id_idx' on column 'id'.");

        // Equality lookup with duplicate key
        System.out.println("\nIndex equality lookup id=2:");
        List<Record> eq = index.lookup("students_id_idx", 2);
        eq.forEach(System.out::println);
        check(eq.size() == 2, "Expected 2 records for id=2");

        // Cross-check index result matches filtered scan
        List<Record> scanFiltered = all.stream()
            .filter(r -> (int) r.getValues().get(0) == 2)
            .collect(Collectors.toList());
        check(equalsIgnoreOrder(eq, scanFiltered), "Index lookup mismatch vs scan filter for id=2");

        // Range lookup
        System.out.println("\nIndex range lookup id in [2,3]:");
        List<Record> rng = index.rangeLookup("students_id_idx", 2, 3);
        rng.forEach(System.out::println);
        check(rng.size() == 3, "Expected 3 records in range [2,3]");

    // Demonstrate random single-record fetch by RID
    RID sampleRid = rids.get(2); // third inserted
        Record sampleRec = storage.read("students", sampleRid);
    System.out.println("\nRandom access via RID (" + sampleRid.pageId() + "," + sampleRid.slotId() + "): " + sampleRec);
    check(sampleRec.getValues().get(1).equals("Bobby"), "RID random access did not fetch expected record");

        // VARCHAR length constraint test (expect failure)
        System.out.println("\nAttempting to insert overly long name (should fail):");
        try {
            String longName = "x".repeat(60); // > 50 bytes
            storage.insert("students", new Record(List.of(5, longName, true)));
            throw new IllegalStateException("Length constraint not enforced for VARCHAR");
        } catch (IllegalArgumentException ex) {
            System.out.println("Caught expected exception: " + ex.getMessage());
        }

        System.out.println("\nAll checks passed. Demo complete.\n");
    }

    private static void deleteIfExists(String path) {
        File f = new File(path);
        if (f.exists()) {
            if (!f.delete()) {
                System.err.println("[WARN] Could not delete existing file: " + path);
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException("CHECK FAILED: " + message);
        }
    }

    private static boolean equalsIgnoreOrder(List<Record> a, List<Record> b) {
        if (a.size() != b.size()) return false;
        // Simple multiset compare via string forms
        List<String> sa = a.stream().map(Object::toString).sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        List<String> sb = b.stream().map(Object::toString).sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        return sa.equals(sb);
    }
}
