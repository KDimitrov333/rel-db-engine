package db.engine.query;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.TestCatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.exec.Row;
import db.engine.index.IndexManager;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

public class QueryProcessorTest {

    private CatalogManager catalog = new TestCatalogManager();
    private StorageManager storage = new StorageManager(catalog);
    private IndexManager index = new IndexManager(catalog, storage);

    private void initSchemas() {
        // Clean previous test table files for isolation
        new java.io.File("target/qp-students.tbl").delete();
        new java.io.File("target/qp-enrollments.tbl").delete();
        TableSchema students = new TableSchema("students", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        ), "target/qp-students.tbl");
        storage.createTable(students);
        TableSchema enrollments = new TableSchema("enrollments", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("student_id", DataType.INT, 0),
            new ColumnSchema("course", DataType.VARCHAR, 50)
        ), "target/qp-enrollments.tbl");
        storage.createTable(enrollments);
    }

    private void seed() {
        List<Record> students = List.of(
            new Record(List.of(1, "Alice", true)),
            new Record(List.of(2, "Bob", false)),
            new Record(List.of(2, "Bobby", true)),
            new Record(List.of(3, "Eve", true)),
            new Record(List.of(4, "Dave", false))
        );
        for (Record r : students) storage.insert("students", r);
        List<Record> enrolls = List.of(
            new Record(List.of(100, 1, "Math")),
            new Record(List.of(101, 1, "Physics")),
            new Record(List.of(102, 2, "Chemistry")),
            new Record(List.of(103, 2, "Biology")),
            new Record(List.of(104, 3, "Math"))
        );
        for (Record r : enrolls) storage.insert("enrollments", r);
        index.createIndex("students_id_idx", "students", "id");
    }

    @Test
    void selectWithIndexAndBooleanColumn() {
        initSchemas();
        seed();
        QueryProcessor qp = new QueryProcessor(catalog, storage, index);
        List<Row> rows = collect(qp.execute("SELECT * FROM students WHERE id = 2"));
        // id=2 appears twice (Bob, Bobby)
        assertEquals(2, rows.size());
        assertTrue(rows.stream().allMatch(r -> r.values().get(0).equals(2)));
    }

    @Test
    void innerJoinBasic() {
        initSchemas();
        seed();
        QueryProcessor qp = new QueryProcessor(catalog, storage, index);
        List<Row> rows = collect(qp.execute("SELECT * FROM students JOIN enrollments ON id = student_id"));
        // Expected join size: 2 (id=1) + 4 (id=2 duplicate left) + 1 (id=3) = 7
        assertEquals(7, rows.size());
    }

    @Test
    void projectionAndWhereAfterJoin() {
        initSchemas();
        seed();
        QueryProcessor qp = new QueryProcessor(catalog, storage, index);
        List<Row> rows = collect(qp.execute("SELECT name, course FROM students JOIN enrollments ON id = student_id WHERE active = true"));
        // active true rows among matched: Alice(x2), Bobby(x2), Eve(x1) => 5 results with those names
        assertEquals(5, rows.size());
        assertTrue(rows.stream().allMatch(r -> r.values().size() == 2));
    }

    private List<Row> collect(Iterable<Row> it) {
        List<Row> list = new ArrayList<>();
        for (Row r : it) list.add(r);
        return list;
    }
}
