package db.engine;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.TableSchema;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;
import db.engine.index.IndexManager;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        CatalogManager catalog = new CatalogManager();
        StorageManager storage = new StorageManager(catalog);
        IndexManager index = new IndexManager(catalog, storage);

        TableSchema schema = new TableSchema(
            "students",
            List.of(
                new ColumnSchema("id", "INT", 0),
                new ColumnSchema("name", "VARCHAR", 50),
                new ColumnSchema("active", "BOOLEAN", 0)
            ),
            "data/students.tbl"
        );

        storage.createTable(schema);
        System.out.println("Created table 'students'");

        storage.insertRecord("students", new Record(List.of(1, "Alice", true)));
        storage.insertRecord("students", new Record(List.of(2, "Bob", false)));
        storage.insertRecord("students", new Record(List.of(3, "Eve", true)));
        System.out.println("Inserted 3 rows into 'students'");

        System.out.println("\nFull table scan:");
        storage.scanTable("students").forEach(System.out::println);

        index.createIndex("students_id_idx", "students", "id");
        System.out.println("\nCreated index on 'id'");

        System.out.println("\nIndex lookup for id=2:");
        index.lookup("students_id_idx", 2).forEach(System.out::println);
    }
}
