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
        banner("Relational DB Engine Demo");

        // Cleanup previous artifacts to make the demo reproducible
        deleteIfExists("data/students.tbl");
        deleteIfExists("indexes/students_id_idx.idx");
        deleteIfExists("catalog/tables.json");
        deleteIfExists("catalog/indexes.json");

        // Boot system
        var catalog = new CatalogManager();
        var storage = new StorageManager(catalog);
        var index = new IndexManager(catalog, storage);

        // Define schema and create table
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
        for (Record r : seed) rids.add(storage.insert("students", r));
        System.out.println("Inserted " + seed.size() + " rows (with duplicate id=2)\n");

        // Full heap scan (RID: record)
        banner("Heap scan");
        List<Record> all = new ArrayList<>();
        storage.scan("students", (rid, rec) -> {
            System.out.println(rid + " : " + rec);
            all.add(rec);
        });
        check(all.size() == seed.size(), "Scan count should equal inserted count");

        // Build index on students(id)
        banner("Create index students_id_idx on students(id)");
        index.createIndex("students_id_idx", "students", "id");
        System.out.println("Index created.\n");

        // Equality lookup (id=2)
        banner("Index equality lookup id=2");
        List<Record> eq2 = index.lookup("students_id_idx", 2);
        eq2.forEach(System.out::println);
        check(eq2.size() == 2, "Expected 2 records for id=2");
        // Cross-check vs scan filter
        List<Record> scanFiltered = all.stream().filter(r -> (int) r.getValues().get(0) == 2).collect(Collectors.toList());
        check(equalsIgnoreOrder(eq2, scanFiltered), "Index lookup mismatch vs scan filter for id=2");

        // Range lookup [2,3]
        banner("Index range lookup id in [2,3]");
        List<Record> rng = index.rangeLookup("students_id_idx", 2, 3);
        rng.forEach(System.out::println);
        check(rng.size() == 3, "Expected 3 records in range [2,3]");

        // Random single-record fetch via RID
        banner("Random access by RID");
        RID sample = rids.get(2); // third inserted (Bobby)
        Record sampleRec = storage.read("students", sample);
        System.out.println(sample + " -> " + sampleRec);
        check(sampleRec.getValues().get(1).equals("Bobby"), "RID random access did not fetch expected record");

        // Delete one row (Bobby) and check index updates
        banner("Delete one row (id=2, name='Bobby') and verify");
        boolean deleted = storage.delete("students", sample);
        check(deleted, "Expected delete to succeed");
        List<Record> eq2After = index.lookup("students_id_idx", 2);
        eq2After.forEach(System.out::println);
        check(eq2After.size() == 1, "After delete, expected 1 record for id=2");

        // VARCHAR length constraint test (expect failure)
        banner("Attempt to insert overly long VARCHAR (should fail)");
        try {
            String longName = "x".repeat(60); // > 50 bytes
            storage.insert("students", new Record(List.of(5, longName, true)));
            throw new IllegalStateException("Length constraint not enforced for VARCHAR");
        } catch (IllegalArgumentException ex) {
            System.out.println("Caught expected: " + ex.getMessage());
        }

        System.out.println("\nDemo complete: inserts, scans, index builds, equality/range lookups, and delete verified.\n");
    }

    private static void banner(String title) {
        System.out.println("\n=== " + title + " ===\n");
    }

    private static void deleteIfExists(String path) {
        File f = new File(path);
        if (f.exists() && !f.delete()) {
            System.err.println("[WARN] Could not delete existing file: " + path);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException("CHECK FAILED: " + message);
    }

    private static boolean equalsIgnoreOrder(List<Record> a, List<Record> b) {
        if (a.size() != b.size()) return false;
        List<String> sa = a.stream().map(Object::toString).sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        List<String> sb = b.stream().map(Object::toString).sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        return sa.equals(sb);
    }
}
