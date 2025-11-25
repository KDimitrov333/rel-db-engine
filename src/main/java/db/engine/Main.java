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
import db.engine.cli.TablePrinter;

public class Main {
    public static void main(String[] args) {
        deleteIfExists("data/students.tbl");
        deleteIfExists("indexes/students_id_idx.idx");
        deleteIfExists("catalog/tables.json");
        deleteIfExists("catalog/indexes.json");
        deleteIfExists("data/enrollments.tbl");

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

        // Second table for join demonstrations: enrollments(student_id -> students.id)
        TableSchema enrollments = new TableSchema(
            "enrollments",
            List.of(
                new ColumnSchema("id", DataType.INT, 0), // enrollment id
                new ColumnSchema("student_id", DataType.INT, 0),
                new ColumnSchema("course", DataType.VARCHAR, 50)
            ),
            "data/enrollments.tbl"
        );
        storage.createTable(enrollments);
        System.out.println("Created table 'enrollments'\n");

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
        System.out.println("Inserted " + seed.size() + " student rows (including duplicate id=2)\n");

        // Seed enrollments referencing student ids
        List<Record> enrollSeed = List.of(
            new Record(List.of(100, 1, "Math")),
            new Record(List.of(101, 1, "Physics")),
            new Record(List.of(102, 2, "Chemistry")),
            new Record(List.of(103, 2, "Biology")),
            new Record(List.of(104, 3, "Math")),
            new Record(List.of(105, 5, "History")),
            new Record(List.of(106, 8, "Math")),
            new Record(List.of(107, 10, "Art"))
        );
        for (Record r : enrollSeed) storage.insert("enrollments", r);
        System.out.println("Inserted " + enrollSeed.size() + " enrollment rows\n");

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
                    Iterable<Row> rowsIter = qp.execute(line);
                    java.util.ArrayList<Row> rows = new java.util.ArrayList<>();
                    for (Row r : rowsIter) rows.add(r);
                    TablePrinter.print(rows);
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
 * Example queries (current supported types: SELECT, INSERT, DELETE, INNER JOIN)
 * Tables:
 *   students(id INT, name VARCHAR, active BOOLEAN)
 *   enrollments(id INT, student_id INT, course VARCHAR)
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
 *
 * INNER JOIN:
 * 1. SELECT * FROM students JOIN enrollments ON id = student_id
 * 2. SELECT name, course FROM students JOIN enrollments ON id = student_id WHERE active = true
 * 3. SELECT id, name FROM students JOIN enrollments ON id = student_id WHERE course = 'Math'
 * ------------------------------------------------------------------------- */
