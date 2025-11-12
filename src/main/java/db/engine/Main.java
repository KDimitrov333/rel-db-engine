package db.engine;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.index.IndexManager;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import db.engine.query.QueryProcessor;
import db.engine.exec.Row;

public class Main {
    public static void main(String[] args) {
        deleteIfExists("data/students.tbl");
        deleteIfExists("indexes/students_id_idx.idx");
        deleteIfExists("catalog/tables.json");
        deleteIfExists("catalog/indexes.json");

        var catalog = new CatalogManager();
        var storage = new StorageManager(catalog);
        var index = new IndexManager(catalog, storage);

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

        List<Record> seed = List.of(
            new Record(List.of(1, "Alice", true)),
            new Record(List.of(2, "Bob", false)),
            new Record(List.of(2, "Bobby", true)),
            new Record(List.of(3, "Eve", true)),
            new Record(List.of(4, "Dave", false)),
            new Record(List.of(5, "Carol", true)),
            new Record(List.of(6, "Frank", false)),
            new Record(List.of(7, "Grace", true)),
            new Record(List.of(8, "Heidi", true)),
            new Record(List.of(9, "Ivan", false)),
            new Record(List.of(10, "Judy", true))
        );
        for (Record r : seed) storage.insert("students", r);
        System.out.println("Inserted " + seed.size() + " rows (including duplicate id=2)\n");

        // Build index on students(id) for optimized equality lookups in queries
        index.createIndex("students_id_idx", "students", "id");
        System.out.println("Index 'students_id_idx' created on students(id).\n");

        System.out.println("Query mode\n");

        // Build query processor (after delete) so users can run queries on current table state.
        QueryProcessor qp = new QueryProcessor(catalog, storage, index);
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("sql> ");
                String line = scanner.nextLine();
                if (line == null) break;
                line = line.trim();
                if (line.equalsIgnoreCase("exit")) {
                    System.out.println("Exiting query mode");
                    break;
                }
                if (line.isEmpty()) continue;
                try {
                    Iterable<Row> rows = qp.execute(line);
                    int count = 0;
                    for (Row r : rows) {
                        System.out.println(r.values());
                        count++;
                        if (count % 1000 == 0) System.out.println("-- fetched " + count + " rows so far");
                    }
                    System.out.println("(" + count + " row(s))");
                } catch (Exception ex) {
                    System.out.println("Error: " + ex.getMessage());
                }
            }
        }
    }

    private static void deleteIfExists(String path) {
        File f = new File(path);
        if (f.exists() && !f.delete()) {
            System.err.println("[WARN] Could not delete existing file: " + path);
        }
    }
}

/* -------------------------------------------------------------------------
 * Example queries (current supported types: SELECT, INSERT, DELETE)
 *
 * SELECT:
 * 1. SELECT * FROM students WHERE id = 2
 * 2. SELECT id, name FROM students WHERE active = true AND id >= 5 AND id <= 8
 * 3. SELECT * FROM students WHERE NOT active OR id < 3
 *
 * INSERT (all columns must be provided in schema order or explicitly listed):
 * 1. INSERT INTO students (id, name, active) VALUES (11, 'Kim', true)
 *
 * DELETE (optionally with WHERE; without WHERE removes all rows):
 * 1. DELETE FROM students WHERE id = 2
 * 2. DELETE FROM students WHERE active = false AND id > 5
 * 3. DELETE FROM students
 * ------------------------------------------------------------------------- */
